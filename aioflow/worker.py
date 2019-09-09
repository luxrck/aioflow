import re
import uuid
import asyncio
from asyncio import ensure_future, get_event_loop, iscoroutine
from inspect import iscoroutinefunction
import time

from multiprocessing import Process
from urllib.parse import urlparse

import redis
import msgpack


class RedisCache(object):
	def __init__(self, id, uri="redis://127.0.0.1:6379/0", persist=False):
		self.__id__ = id
		self.__persist__ = persist
		self.__iters__ = {}
		uri = urlparse(uri)
		host, port= uri.netloc.split(":")
		db = re.findall(r"\d+", uri.path)[0]
		self.cache = redis.Redis(host=host, port=port, db=db)


	def pull(self, key, num=1):
		key = self.key(key)
		if not self.__persist__:
			return [msgpack.unpackb(self.cache.spop(key), encoding='utf-8') for i in range(num) if self.cache.scard(key)]

		if self.__iters__.get(key, -1) == 0:
			del self.__iters__[key]; return []
		self.__iters__.setdefault(key, 0)
		seed, data = self.cache.sscan(key, self.__iters__[key], count=num)
		self.__iters__[key] = seed
		print("cache:", key, seed, len(data))
		return [msgpack.unpackb(v, encoding='utf-8') for v in data]


	def push(self, key, values):
		if not values: return
		self.cache.sadd(self.key(key), *[msgpack.packb(value) for value in values])


	def remove(self, key, value):
		self.cache.srem(self.key(key), msgpack.packb(value))


	def info(self, key):
		return {"location": key, "size": self.cache.scard(self.key(key))}


	def key(self, key):
		return self.__id__ + ":" + key


	def update(self, d):
		for k,v in d.items():
			self.push(k, v)


class App(object):

	class Meta(object):
		def __init__(self, d):
			Meta = App.Meta
			for k, v in d.items():
				if isinstance(v, (list, tuple)):
					setattr(self, k, [Meta(i) if isinstance(i, (dict)) else i for i in v])
				else:
					setattr(self, k, Meta(v) if isinstance(v, (dict)) else v)


	class Coro(object):
		def __init__(self, target, name="", args=True, parallel=1, batch=1, src=None, interval=30000):
			self.args = args
			self.parallel = parallel
			self.batch = batch
			self.count = 0
			self.src = src
			self.interval = interval
			self.lasttime = 0
			self.runnable = True
			self.target = target
			self.name = name
			if not self.name: self.name = self.target.__name__


		def run(self, *args):
			self.count += 1
			async def wrapper(self, *args):
				try:
					await self.target(*args)
					# import pdb; pdb.set_trace()
				except KeyboardInterrupt:
					import sys; sys.exit()
				except Exception as e:
					print("Woker.Coro.run:", "target:", self.name, ",", e)
				self.count -= 1
			return wrapper(self, *args)


	def __init__(self, id, cache_uri='redis://127.0.0.1:6379/0', cache_persist=False, executor=None):
		self.__id__ = id
		# self.__closing__ = False
		# self.__closed__ = False
		self.__meta__ = None
		self.__final__ = None
		self.__boot__ = []
		self.__coroutines__ = []
		self.closing = False
		self.cache = RedisCache(id, uri=cache_uri, persist=cache_persist)
		# self.loop = get_event_loop() if not loop else loop
		self.executor = executor


	def init(self, func=None):
		async def __meta__():
			results = (await func()) if func else {}
			if not results: results = {}
			meta = App.Meta(results)
			# meta.loop = self.loop
			# self.loop.set_default_executor(self.executor)
			return meta
		self.__meta__ = __meta__
		return __meta__


	def unit(self, func):
		async def __unit__(meta, *args):
			if iscoroutinefunction(func):
				results = await func(meta, *args)
			elif callable(func):
				results = await meta.loop.run_in_executor(self.executor, func, meta, *args)
			if results:
				self.cache.update(results)
		return __unit__


	def boot(self, func=None, interval=500000):
		def wrap(func):
			__begin__ = self.unit(func)
			co = App.Coro(target=__begin__, name=func.__name__, args=False, parallel=1, batch=1, src=None, interval=interval)
			self.__boot__.append(co)
			return co
		if not func: return wrap
		return wrap(func)


	def on(self, src, parallel=1, batch=1, interval=3000):
		def wrap(func):
			__on__ = self.unit(func)
			co = App.Coro(target=__on__, name=func.__name__, args=True, parallel=parallel, batch=batch, src=src, interval=interval)
			self.__coroutines__.append(co)
			return co
		return wrap


	def final(self, func):
		async def __final__(meta):
			results = await func(meta)
		self.__final__ = __final__
		return __final__


	def t(self, coro, meta, forever):
		c = coro
		loop = meta.loop
		if iscoroutine(coro):
			return ensure_future(loop.create_task(coro))
		# print([type(c), c.name, c.src, c.lasttime, c.interval, c.count, c.parallel, c.batch])
		if not coro.runnable: return
		if forever:
			if time.time() - coro.interval < coro.lasttime: return
		if coro.count < coro.parallel:
			if not coro.args:
				loop.create_task(coro.run(meta))
			else:
				args = self.cache.pull(coro.src, coro.batch * coro.parallel)
				if not args: coro.lasttime = time.time(); return
				for i in range(coro.parallel):
					cargs = args[i*coro.batch: (i+1)*coro.batch]
					if not cargs: break
					loop.create_task(coro.run(meta, *cargs))


	def __call__(self, boot=True, forever=False):
		async def __run__(self, boot, forever):
			meta = await self.__meta__()
			meta.loop = get_event_loop()	# get the event loop which we are running inside it.
			if boot:
				for b in self.__boot__: self.t(b, meta, forever)
			__closing__ = __closed__ = False
			while not __closed__:
				if not __closing__:
					for coro in self.__coroutines__: self.t(coro, meta, forever)
				if not forever:
					tasks = sum([c.count for c in self.__coroutines__ + self.__boot__])
					if not tasks: __closing__ = True
				if __closing__ or self.closing:
					if not __closed__:
						if self.__final__: await self.t(self.__final__(meta), meta, False)
						__closed__ = True
				await asyncio.sleep(0.2)
		return __run__(self, boot, forever)

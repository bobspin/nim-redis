import redis, unittest, asyncdispatch, std/[os, strutils, options, net]

#[
Redis connection and database for sync and async tests can be configured with
environment variables NIM_REDIS_TEST_URL and NIM_REDIS_TEST_DB.
    - if NIM_REDIS_TEST_URL is not set "localhost" will be used as default
    - if NIM_REDIS_TEST_URL starts with unix:// a unix socket connection will be used
    - NIM_REDIS_TEST_URL can optionally include a port (e.g., "localhost:16379")
    - if NIM_REDIS_TEST_DB is not set or empty, no redis.select() will be executed
    - if either of these variables has an invalid value, tests will fail with an error
Example for tests via socket /run/redis/redis.sock using database 15:
    export NIM_REDIS_TEST_URL=unix:///run/redis/redis.sock
    export NIM_REDIS_TEST_DB=15
    nimble test
]#

type ConnectionConfig = tuple
  # default host if NIM_REDIS_TEST_URL is not set or empty
  default: string
  # host from NIM_REDIS_TEST_URL (if not unix socket)
  host: Option[string]
  # will only be some if host is some
  port: Option[Port]
  # socket from NIM_REDIS_TEST_URL if it starts with unix://
  sock: Option[string]
  # database index from NIM_REDIS_TEST_DB
  db: Option[int]

proc getConnectionConfig(): ConnectionConfig =
  ## Parses Redis connection from environment variables `NIM_REDIS_TEST_{URL,DB}`.
  ## Invalid configurations in variables will intentionally raise errors.
  result.default = "localhost"
  let url = getEnv("NIM_REDIS_TEST_URL", "")
  if url != "":
    if url.startsWith("unix://"):
      result.sock = some(url["unix://".len .. ^1])
    elif ':' in url:
      let parts = url.split(':')
      result.host = some(parts[0])
      result.port = some(Port(parts[1].parseInt))
    else:
      result.host = some(url)
  let dbStr = getEnv("NIM_REDIS_TEST_DB", "")
  if dbStr != "":
    result.db = some(dbStr.parseInt)

proc connectSync(): Redis =
  ## Establishes a synchronous Redis connection for tests.
  let config = getConnectionConfig()
  if config.host.isSome:
    if config.port.isSome:
      result = redis.open(config.host.get, config.port.get)
    else:
      result = redis.open(config.host.get)
  elif config.sock.isSome:
    result = redis.openUnix(config.sock.get)
  else:
    result = redis.open(config.default)
  if config.db.isSome:
    discard result.select(config.db.get)
  return result

proc connectAsyncAndSetDB(): Future[AsyncRedis] {.async.} =
  ## Helper to create an asynchronous Redis connection.
  let config = getConnectionConfig()
  if config.host.isSome:
    if config.port.isSome:
      result = await redis.openAsync(config.host.get, config.port.get)
    else:
      result = await redis.openAsync(config.host.get)
  elif config.sock.isSome:
    result = await redis.openUnixAsync(config.sock.get)
  else:
    result = await redis.openAsync(config.default)
  if config.db.isSome:
    discard await result.select(config.db.get)
  return result

proc connectAsync(): Future[AsyncRedis] =
  ## Establishes an asynchronous Redis connection for tests.
  ## Wraps `connectAsyncAndSetDB` to provide a clean `Future[AsyncRedis]`.
  return connectAsyncAndSetDB()

template syncTests() =
  let r = connectSync()
  let keys = r.keys("*")
  doAssert keys.len == 0, "Don't want to mess up an existing DB."

  test "simple set and get":
    const expected = "Hello, World!"

    r.setk("redisTests:simpleSetAndGet", expected)
    let actual = r.get("redisTests:simpleSetAndGet")

    check actual == expected

  test "get returns values byte-for-byte":
    # Regression test for https://github.com/nim-lang/redis/issues/44:
    # bulk replies were passed through strip(), corrupting values with
    # leading/trailing whitespace. Bulk strings are binary safe, so
    # embedded CRLF and NUL bytes must survive the round trip too.
    const expected = " \t padded\r\nvalue\0 \n "

    r.setk("redisTests:whitespacePreserved", expected)
    let actual = r.get("redisTests:whitespacePreserved")

    check actual == expected

  test "increment key by one":
    const expected = 3

    r.setk("redisTests:incrementKeyByOne", "2")
    let actual = r.incr("redisTests:incrementKeyByOne")

    check actual == expected

  test "increment key by five":
    const expected = 10

    r.setk("redisTests:incrementKeyByFive", "5")
    let actual = r.incrBy("redisTests:incrementKeyByFive", 5)

    check actual == expected

  test "decrement key by one":
    const expected = 2

    r.setk("redisTest:decrementKeyByOne", "3")
    let actual = r.decr("redisTest:decrementKeyByOne")

    check actual == expected

  test "decrement key by three":
    const expected = 7

    r.setk("redisTest:decrementKeyByThree", "10")
    let actual = r.decrBy("redisTest:decrementKeyByThree", 3)

    check actual == expected

  test "append string to key":
    const expected = "hello world"

    r.setk("redisTest:appendStringToKey", "hello")
    let keyLength = r.append("redisTest:appendStringToKey", " world")

    check keyLength == len(expected)
    check r.get("redisTest:appendStringToKey") == expected

  test "check key exists":
    r.setk("redisTest:checkKeyExists", "foo")
    check r.exists("redisTest:checkKeyExists") == true

  test "delete key":
    r.setk("redisTest:deleteKey", "bar")
    check r.exists("redisTest:deleteKey") == true

    check r.del(@["redisTest:deleteKey"]) == 1
    check r.exists("redisTest:deleteKey") == false

  test "rename key":
    const expected = "42"

    r.setk("redisTest:renameKey", expected)
    discard r.rename("redisTest:renameKey", "redisTest:meaningOfLife")

    check r.exists("redisTest:renameKey") == false
    check r.get("redisTest:meaningOfLife") == expected

  test "get key length":
    const expected = 5

    r.setk("redisTest:getKeyLength", "hello")
    let actual = r.strlen("redisTest:getKeyLength")

    check actual == expected

  test "push entries to list":
    for i in 1..5:
      check r.lPush("redisTest:pushEntriesToList", $i) == i

    check r.llen("redisTest:pushEntriesToList") == 5

  test "pfcount supports single key and multiple keys":
    discard r.pfadd("redisTest:pfcount1", @["foo"])
    check r.pfcount("redisTest:pfcount1") == 1

    discard r.pfadd("redisTest:pfcount2", @["bar"])
    check r.pfcount(@["redisTest:pfcount1", "redisTest:pfcount2"]) == 2

  test "flushPipeline preserves values containing OK or QUEUED":
    # Regression test for https://github.com/nim-lang/redis/issues/48:
    # pipeline results were filtered with contains("OK")/contains("QUEUED"),
    # a substring match, so legitimate values were silently dropped and
    # later results shifted position.
    const
      lookupKey = "redisTests:pipeline:lookup"
      jobsKey = "redisTests:pipeline:jobs"
      plainKey = "redisTests:pipeline:plain"

    r.setk(lookupKey, "LOOKUP")
    r.setk(jobsKey, "QUEUED_JOBS")
    r.setk(plainKey, "plain")

    r.startPipelining()
    discard r.get(lookupKey)
    discard r.get(jobsKey)
    discard r.get(plainKey)
    let res = r.flushPipeline()

    check res == @["LOOKUP", "QUEUED_JOBS", "plain"]

  test "flushPipeline preserves values equal to OK or QUEUED":
    # Status acknowledgments are filtered by RESP reply type, so a data
    # reply whose text is exactly "OK" or "QUEUED" must survive.
    const
      okKey = "redisTests:pipeline:okval"
      queuedKey = "redisTests:pipeline:queuedval"

    r.setk(okKey, "OK")
    r.setk(queuedKey, "QUEUED")

    r.startPipelining()
    discard r.get(okKey)
    discard r.get(queuedKey)
    let res = r.flushPipeline()

    check res == @["OK", "QUEUED"]

  test "exec preserves values containing OK":
    const brokenKey = "redisTests:multi:broken"

    r.setk(brokenKey, "BROKEN")

    r.multi()
    discard r.get(brokenKey)
    let res = r.exec()

    check res == @["BROKEN"]

  test "exec preserves values equal to OK":
    const okKey = "redisTests:multi:okval"

    r.setk(okKey, "OK")

    r.multi()
    discard r.get(okKey)
    let res = r.exec()

    check res == @["OK"]

  # TODO: Ideally tests for all other procedures, will add these in the future

  # delete all keys in the DB at the end of the tests
  discard r.flushdb()
  r.quit()
suite "redis tests":
  syncTests()

suite "redis async tests":
  let r = waitFor connectAsync()
  let keys = waitFor r.keys("*")
  doAssert keys.len == 0, "Don't want to mess up an existing DB."

  test "issue #6":
    # See `tawaitorder` for a test that doesn't depend on Redis.
    const count = 5
    proc retr(key: string, expect: string) {.async.} =
      let val = await r.get(key)

      doAssert val == expect

    proc main(): Future[bool] {.async.} =
      for i in 0 ..< count:
        await r.setk("key" & $i, "value" & $i)

      var futures: seq[Future[void]] = @[]
      for i in 0 ..< count:
        futures.add retr("key" & $i, "value" & $i)

      for fut in futures:
        await fut

      return true

    check (waitFor main())

  test "pub/sub":

    proc main() {.async.} =
      let sub = await connectAsync()
      let pub = await connectAsync()

      let listerns = await pub.publish("channel1", "hi there")
      doAssert listerns == 0

      await sub.subscribe("channel1")
      # you should only call sub.nextMessage() from now on

      discard await pub.publish("channel1", "one")
      discard await pub.publish("channel1", "two")
      discard await pub.publish("channel1", "three")

      doAssert (await sub.nextMessage()).message == "one"
      doAssert (await sub.nextMessage()).message == "two"
      doAssert (await sub.nextMessage()).message == "three"

    waitFor main()

  discard waitFor r.flushdb()
  waitFor r.quit()


when compileOption("threads"):
  proc threadFunc() {.thread.} =
    suite "redis threaded tests":
      syncTests()

  var th: Thread[void]
  createThread(th, threadFunc)
  joinThread(th)

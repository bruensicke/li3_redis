<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\tests\cases\storage;

use li3_redis\storage\Redis;
use li3_redis\storage\Stats;

use lithium\data\Connections;
use Redis as RedisCore;

class StatsTest extends \lithium\test\Unit {

	public $redis;

	public function skip() {
		$this->skipIf(!Redis::enabled(), 'The Redis extension is not loaded!');

		$this->_connectionConfig = Connections::get('li3_redis', array('config' => true));
		$hasDb = (isset($this->_connectionConfig['type']) && $this->_connectionConfig['type'] == 'Redis');
		$message = 'Test database is either unavailable, or not a Redis connection!';
		$this->skipIf(!$hasDb, $message);
	}

	public function setUp() {
		$this->redis = new RedisCore();
		$this->redis->connect('127.0.0.1', 6379);
		$this->redis->select(1);
		$this->redis->flushDB();
	}

	public function tearDown() {
		$this->redis->select(1);
		$this->redis->flushDB();
	}

	function testInc() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));

		// simplest call
		$expected = array('bar' => 1);
		$result = Stats::inc('foo', 'bar');
		$inRedis = $this->redis->hGetAll("$scope:stats:global:foo");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis);

		// simple call, adding second field
		$expected = array('bar' => 1, 'baz' => 1);
		$result = Stats::inc('foo', 'baz');
		$inRedis = $this->redis->hGetAll("$scope:stats:global:foo");
		$this->assertEqual(array('baz' => 1), $result);
		$this->assertEqual($expected, $inRedis);

		// simple call, increment second value
		$expected = array('bar' => 1, 'baz' => 2);
		$result = Stats::inc('foo', 'baz');
		$inRedis = $this->redis->hGetAll("$scope:stats:global:foo");
		$this->assertEqual(array('baz' => 2), $result);
		$this->assertEqual($expected, $inRedis);

		// multiple at once
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::inc('multi', array('field1' => 1, 'field2' => 1));
		$inRedis = $this->redis->hGetAll("$scope:stats:global:multi");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis);

		// incrementing multiple at once
		$expected = array('field1' => 2, 'field2' => 3);
		$result = Stats::inc('multi', array('field1' => 1, 'field2' => 2));
		$inRedis = $this->redis->hGetAll("$scope:stats:global:multi");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis);

		// multiple at once, with plain bucket
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::inc('withBucket', $expected, 'prefix');
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:withBucket");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:prefix:withBucket");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis1);
		$this->assertEqual($expected, $inRedis2);

		// multiple buckets at once
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::inc('multiBucket', $expected, array('prefix1', 'prefix2'));
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:multiBucket");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:prefix1:multiBucket");
		$inRedis3 = $this->redis->hGetAll("$scope:stats:prefix2:multiBucket");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis1);
		$this->assertEqual($expected, $inRedis2);
		$this->assertEqual($expected, $inRedis3);

		// multiple at once, with one bucket in array
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::inc('withBucketArray', $expected, array('user' => 'foo'));
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:withBucketArray");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:user:foo:withBucketArray");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis1);
		$this->assertEqual($expected, $inRedis2);

		// multiple buckets as associated array
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::inc('multiBucketArray', $expected, array('user' => 'foo', 'year' => '2013'));
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:multiBucketArray");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:user:foo:multiBucketArray");
		$inRedis3 = $this->redis->hGetAll("$scope:stats:year:2013:multiBucketArray");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis1);
		$this->assertEqual($expected, $inRedis2);
		$this->assertEqual($expected, $inRedis3);

		// multiple buckets as associated array with return all
		$expected = array(
			'global' => array('field1' => 2, 'field2' => 2),
			'prefix1' => array('field1' => 2, 'field2' => 2),
			'prefix2' => array('field1' => 2, 'field2' => 2),
		);
		$result = Stats::inc('multiBucket',
			array('field1' => 1, 'field2' => 1),
			array('prefix1', 'prefix2'),
			array('all' => true)
		);
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:multiBucket");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:prefix1:multiBucket");
		$inRedis3 = $this->redis->hGetAll("$scope:stats:prefix2:multiBucket");
		$this->assertEqual($expected, $result);
		$this->assertEqual(array('field1' => 2, 'field2' => 2), $inRedis1);
		$this->assertEqual(array('field1' => 2, 'field2' => 2), $inRedis2);
		$this->assertEqual(array('field1' => 2, 'field2' => 2), $inRedis3);

		// multiple buckets as associated array with return all
		$expected = array(
			'global' => array('field1' => 2, 'field2' => 2),
			'user:foo' => array('field1' => 2, 'field2' => 2),
			'year:2013' => array('field1' => 2, 'field2' => 2),
		);
		$result = Stats::inc('multiBucketArray',
			array('field1' => 1, 'field2' => 1),
			array('user' => 'foo', 'year' => '2013'),
			array('all' => true)
		);
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:multiBucketArray");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:user:foo:multiBucketArray");
		$inRedis3 = $this->redis->hGetAll("$scope:stats:year:2013:multiBucketArray");
		$this->assertEqual($expected, $result);
		$this->assertEqual(array('field1' => 2, 'field2' => 2), $inRedis1);
		$this->assertEqual(array('field1' => 2, 'field2' => 2), $inRedis2);
		$this->assertEqual(array('field1' => 2, 'field2' => 2), $inRedis3);
	}

	function testDelete() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$data1 = array('calls' => 140, 'fails' => 22, 'successful' => 118);
		$data2 = array('calls' => 35, 'fails' => 3, 'successful' => 32);
		$data3 = array('calls' => 28, 'fails' => 16, 'successful' => 12);
		$this->assertTrue($this->redis->hMset("$scope:stats:global:foo", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix1:foo", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix2:foo", $data3));
		$this->assertTrue($this->redis->hMset("$scope:stats:global:bar", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:user:foo:bar", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:year:2013:bar", $data3));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:bar"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:user:foo:bar"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:year:2013:bar"));

		// simplest call (except delete key at all)
		$expected = $data1;
		unset($expected['calls']);
		$result = Stats::delete('foo', 'calls');
		$this->assertEqual(1, $result);
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));

		// array call, one field
		$expected = $data1;
		unset($expected['calls']);
		unset($expected['fails']);
		$result = Stats::delete('foo', array('fails'));
		$this->assertEqual(1, $result);
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));

		// array call, two fields
		$expected = $data1;
		unset($expected['calls']);
		unset($expected['successful']);
		$result = Stats::delete('bar', array('calls', 'successful'));
		$this->assertEqual(2, $result);
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:bar"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:user:foo:bar"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:year:2013:bar"));

		/**
		 * start over again
		 */
		$this->assertTrue($this->redis->hMset("$scope:stats:global:foo", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix1:foo", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix2:foo", $data3));
		$this->assertTrue($this->redis->hMset("$scope:stats:global:bar", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:user:foo:bar", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:year:2013:bar", $data3));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:bar"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:user:foo:bar"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:year:2013:bar"));

		// array call, two fields, one bucket
		$expected = $data2;
		unset($expected['calls']);
		unset($expected['successful']);
		$result = Stats::delete('foo', array('calls', 'successful'), 'prefix1');
		$this->assertEqual(2, $result);
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));

		/**
		 * start over again
		 */
		$this->assertTrue($this->redis->hMset("$scope:stats:global:foo", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix1:foo", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix2:foo", $data3));
		$this->assertTrue($this->redis->hMset("$scope:stats:global:bar", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:user:foo:bar", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:year:2013:bar", $data3));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:bar"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:user:foo:bar"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:year:2013:bar"));

		// array call, one field, two buckets
		$expected1 = $data2;
		unset($expected1['fails']);
		$expected2 = $data3;
		unset($expected2['fails']);
		$result = Stats::delete('foo', 'fails', array('prefix1', 'prefix2'));
		$this->assertEqual(2, $result);
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($expected1, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($expected2, $this->redis->hGetAll("$scope:stats:prefix2:foo"));

		/**
		 * start over again
		 */
		$this->assertTrue($this->redis->hMset("$scope:stats:global:foo", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix1:foo", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix2:foo", $data3));
		$this->assertTrue($this->redis->hMset("$scope:stats:global:bar", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:user:foo:bar", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:year:2013:bar", $data3));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:bar"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:user:foo:bar"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:year:2013:bar"));

		// delete key
		$result = Stats::delete('foo');
		$this->assertEqual(1, $result);
		$this->assertFalse($this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));
	}

	function testSet() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));

		// simplest call
		$expected = array('bar' => 'baz');
		$result = Stats::set('foo', array('bar' => 'baz'));
		$inRedis = $this->redis->hGetAll("$scope:stats:global:foo");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis);

		// simple call, adding second field
		$expected = array('bar' => 'baz', 'baz' => 'bak');
		$result = Stats::set('foo', array('baz' => 'bak'));
		$inRedis = $this->redis->hGetAll("$scope:stats:global:foo");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis);

		// simple call, overwrite second value
		$expected = array('bar' => 'baz', 'baz' => 'bam');
		$result = Stats::set('foo', array('baz' => 'bam'));
		$inRedis = $this->redis->hGetAll("$scope:stats:global:foo");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis);

		// multiple at once
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::set('multi', array('field1' => 1, 'field2' => 1));
		$inRedis = $this->redis->hGetAll("$scope:stats:global:multi");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis);

		// overwriting multiple at once
		$expected = array('field1' => 1, 'field2' => 2);
		$result = Stats::set('multi', array('field1' => 1, 'field2' => 2));
		$inRedis = $this->redis->hGetAll("$scope:stats:global:multi");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis);

		// multiple at once, with plain bucket
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::set('withBucket', $expected, 'prefix');
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:withBucket");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:prefix:withBucket");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis1);
		$this->assertEqual($expected, $inRedis2);

		// multiple buckets at once
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::set('multiBucket', $expected, array('prefix1', 'prefix2'));
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:multiBucket");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:prefix1:multiBucket");
		$inRedis3 = $this->redis->hGetAll("$scope:stats:prefix2:multiBucket");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis1);
		$this->assertEqual($expected, $inRedis2);
		$this->assertEqual($expected, $inRedis3);

		// multiple at once, with one bucket in array
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::set('withBucketArray', $expected, array('user' => 'foo'));
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:withBucketArray");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:user:foo:withBucketArray");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis1);
		$this->assertEqual($expected, $inRedis2);

		// multiple buckets as associated array
		$expected = array('field1' => 1, 'field2' => 1);
		$result = Stats::set('multiBucketArray', $expected, array('user' => 'foo', 'year' => '2013'));
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:multiBucketArray");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:user:foo:multiBucketArray");
		$inRedis3 = $this->redis->hGetAll("$scope:stats:year:2013:multiBucketArray");
		$this->assertEqual($expected, $result);
		$this->assertEqual($expected, $inRedis1);
		$this->assertEqual($expected, $inRedis2);
		$this->assertEqual($expected, $inRedis3);

		// multiple buckets as associated array with return all
		$expected = array(
			'global' => array('field1' => 'foo', 'field2' => 'foo'),
			'prefix1' => array('field1' => 'foo', 'field2' => 'foo'),
			'prefix2' => array('field1' => 'foo', 'field2' => 'foo'),
		);
		$result = Stats::set('multiBucket',
			array('field1' => 'foo', 'field2' => 'foo'),
			array('prefix1', 'prefix2'),
			array('all' => true)
		);
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:multiBucket");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:prefix1:multiBucket");
		$inRedis3 = $this->redis->hGetAll("$scope:stats:prefix2:multiBucket");
		$this->assertEqual($expected, $result);
		$this->assertEqual(array('field1' => 'foo', 'field2' => 'foo'), $inRedis1);
		$this->assertEqual(array('field1' => 'foo', 'field2' => 'foo'), $inRedis2);
		$this->assertEqual(array('field1' => 'foo', 'field2' => 'foo'), $inRedis3);

		// multiple buckets as associated array with return all
		$expected = array(
			'global' => array('field1' => 'foobar', 'field2' => 'foobar'),
			'user:foo' => array('field1' => 'foobar', 'field2' => 'foobar'),
			'year:2013' => array('field1' => 'foobar', 'field2' => 'foobar'),
		);
		$result = Stats::set('multiBucket',
			array('field1' => 'foobar', 'field2' => 'foobar'),
			array('user' => 'foo', 'year' => '2013'),
			array('all' => true)
		);
		$inRedis1 = $this->redis->hGetAll("$scope:stats:global:multiBucket");
		$inRedis2 = $this->redis->hGetAll("$scope:stats:user:foo:multiBucket");
		$inRedis3 = $this->redis->hGetAll("$scope:stats:year:2013:multiBucket");
		$this->assertEqual($expected, $result);
		$this->assertEqual(array('field1' => 'foobar', 'field2' => 'foobar'), $inRedis1);
		$this->assertEqual(array('field1' => 'foobar', 'field2' => 'foobar'), $inRedis2);
		$this->assertEqual(array('field1' => 'foobar', 'field2' => 'foobar'), $inRedis3);
	}

	function testGet() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$data1 = array('calls' => 140, 'fails' => 22, 'successful' => 118);
		$data2 = array('calls' => 35, 'fails' => 3, 'successful' => 32);
		$data3 = array('calls' => 28, 'fails' => 16, 'successful' => 12);
		$this->assertTrue($this->redis->hMset("$scope:stats:global:foo", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix1:foo", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix2:foo", $data3));
		$this->assertTrue($this->redis->hMset("$scope:stats:global:bar", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:user:foo:bar", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:year:2013:bar", $data3));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:bar"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:user:foo:bar"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:year:2013:bar"));

		// simplest call
		$this->assertEqual($data1, Stats::get('foo'));

		// with one bucket
		$result = Stats::get('foo', 'prefix1');
		$this->assertEqual($data2, $result);

		// with one bucket as array
		$result = Stats::get('foo', array('prefix1'));
		$this->assertEqual($data2, $result);

		// with two buckets as flat array
		$expected = array(
			'prefix1' => $data2,
			'prefix2' => $data3,
		);
		$result = Stats::get('foo', array('prefix1', 'prefix2'));
		$this->assertEqual($expected, $result);

		// with two buckets as flat array
		$expected = array(
			'global' => $data1,
			'prefix1' => $data2,
			'prefix2' => $data3,
		);
		$result = Stats::get('foo', array('global', 'prefix1', 'prefix2'));
		$this->assertEqual($expected, $result);

		// with one bucket as associative array
		$result = Stats::get('bar', array('user' => 'foo'));
		$this->assertEqual($data2, $result);

		// with two buckets as associative array
		$expected = array('user:foo' => $data2, 'year:2013' => $data3);
		$result = Stats::get('bar', array('user' => 'foo', 'year' => 2013));
		$this->assertEqual($expected, $result);

		// with mixed buckets
		$expected = array('global' => $data1, 'user:foo' => $data2);
		$result = Stats::get('bar', array('user' => 'foo', 'global'));
		$this->assertEqual($expected, $result);

		// with one field
		$result = Stats::get('foo', 'global', 'calls');
		$this->assertEqual($data1['calls'], $result);

		// with one field for one flat bucket
		$result = Stats::get('foo', 'prefix1', 'calls');
		$this->assertEqual($data2['calls'], $result);

		// with one field for a different bucket
		$result = Stats::get('bar', 'user:foo', 'calls');
		$this->assertEqual($data2['calls'], $result);

		// with one field for an arrayed bucket
		$result = Stats::get('bar', array('user' => 'foo'), 'calls');
		$this->assertEqual($data2['calls'], $result);

		// with one field for an arrayed bucket
		$expected = array(
			'prefix1' => $data2['calls'],
			'prefix2' => $data3['calls'],
		);
		$result = Stats::get('foo', array('prefix1', 'prefix2'), 'calls');
		$this->assertEqual($expected, $result);

		// with one field for an arrayed bucket
		$expected = array(
			'prefix1' => $data2['fails'],
			'prefix2' => $data3['fails'],
		);
		$result = Stats::get('foo', array('prefix1', 'prefix2'), 'fails');
		$this->assertEqual($expected, $result);

		// with one field for an arrayed bucket
		$expected = array(
			'user:foo' => $data2['fails'],
			'year:2013' => $data3['fails'],
		);
		$result = Stats::get('bar', array('user' => 'foo', 'year' => '2013'), 'fails');
		$this->assertEqual($expected, $result);

		// with one fields
		$result = Stats::get('bar', 'global', 'successful');
		$this->assertEqual($data1['successful'], $result);

		// with two fields
		$result = Stats::get('bar', 'global', array('successful', 'calls'));
		$this->assertEqual(array('calls' => 140, 'successful' => 118), $result);
		$this->assertFalse(isset($result['fails']));

		// with one field for different bucket
		$result = Stats::get('foo', 'prefix1', 'fails');
		$this->assertEqual($data2['fails'], $result);

		// with one field for different bucket
		$result = Stats::get('bar', array('year' => 2013), 'successful');
		$this->assertEqual($data3['successful'], $result);

		// with two fields and two buckets
		$expected = array(
			'global' => array('calls' => 140, 'successful' => 118),
			'prefix1' => array('calls' => 35, 'successful' => 32),
		);
		$result = Stats::get('foo', array('global', 'prefix1'), array('successful', 'calls'));
		$this->assertEqual($expected, $result);
		$this->assertFalse(isset($result['global']['fails']));
		$this->assertFalse(isset($result['prefix1']['fails']));
		$this->assertFalse(isset($result['prefix2']));

		// with two fields and two associative buckets
		$expected = array(
			'user:foo' => array('fails' => 3, 'successful' => 32),
			'year:2013' => array('fails' => 16, 'successful' => 12),
		);
		$result = Stats::get('bar', array('user' => 'foo', 'year' => 2013), array('successful', 'fails'));
		$this->assertEqual($expected, $result);
		$this->assertFalse(isset($result['user:foo']['calls']));
		$this->assertFalse(isset($result['year:2013']['calls']));
		$this->assertFalse(isset($result['global']));
	}

	function testValues() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$data1 = array('calls' => 140, 'fails' => 22);
		$data2 = array('calls' => 35, 'fails' => 3);
		$data3 = array('calls' => 17, 'fails' => 29);
		$this->assertTrue($this->redis->hMset("$scope:stats:global:foo", $data1));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix1:foo", $data2));
		$this->assertTrue($this->redis->hMset("$scope:stats:prefix2:foo", $data3));
		$this->assertEqual($data1, $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));

		// simplest call
		$expected = array('calls' => 140, 'fails' => 22);
		$result = Stats::values('foo');
		$this->assertEqual($expected, $result);

		// with one prefix
		$expected = array('calls' => 35, 'fails' => 3);
		$result = Stats::values('foo', '', array('prefix' => 'prefix1'));
		$this->assertEqual($expected, $result);

		// with one field
		$expected = 140;
		$result = Stats::values('foo', 'calls');
		$this->assertEqual($expected, $result);

		// with one field and prefix
		$expected = 35;
		$result = Stats::values('foo', 'calls', array('prefix' => 'prefix1'));
		$this->assertEqual($expected, $result);

		// with no fields and prefix
		$expected = array('calls' => 35, 'fails' => 3);
		$result = Stats::values('foo', '', array('prefix' => 'prefix1'));
		$this->assertEqual($expected, $result);

		// with no fields and prefix
		$expected = array('calls' => 17, 'fails' => 29);
		$result = Stats::values('foo', '', array('prefix' => 'prefix2'));
		$this->assertEqual($expected, $result);
	}
}
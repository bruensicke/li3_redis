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

use Redis as RedisCore;

class StatsTest extends \lithium\test\Unit {

	public $redis;

	public function skip() {
		$this->skipIf(!Redis::enabled(), 'The Redis extension is not loaded!');
	}

	public function setUp() {
		$this->redis = new RedisCore();
		$this->redis->connect('127.0.0.1', 6379);
		$this->redis->select(1);
		$this->redis->flushDB();
		Redis::connection($this->redis);
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
		$this->assertEqual($expected, Stats::inc('foo', 'bar'));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));

		// simplest call
		$expected = array('bar' => 2);
		$this->assertEqual($expected, Stats::inc('foo', 'bar'));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));

		// simple call, adding second field
		$expected = array('bar' => 2, 'baz' => 1);
		$this->assertEqual(array('baz' => 1), Stats::inc('foo', 'baz'));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));

		// simple call, increment second value
		$expected = array('bar' => 2, 'baz' => 2);
		$this->assertEqual(array('baz' => 2), Stats::inc('foo', 'baz'));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));

		// multiple at once as flat array
		// would be cool to have!
		// $expected = array('field1' => 1, 'field2' => 1);
		// $this->assertEqual($expected, Stats::inc('multiFlat', array('field1', 'field2')));
		// $this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:multiFlat"));

		// multiple at once
		$expected = array('field1' => 1, 'field2' => 1);
		$this->assertEqual($expected, Stats::inc('multi', array('field1' => 1, 'field2' => 1)));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:multi"));

		// incrementing multiple at once
		$expected = array('field1' => 2, 'field2' => 3);
		$this->assertEqual($expected, Stats::inc('multi', array('field1' => 1, 'field2' => 2)));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:multi"));

		// multiple at once, with plain bucket
		$expected = array('field1' => 1, 'field2' => 1);
		$this->assertEqual($expected, Stats::inc('withBucket', $expected, 'prefix'));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:withBucket"));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:prefix:withBucket"));

		// multiple buckets at once
		$data = array('field1' => 1, 'field2' => 1);
		$expected = array(
			'prefix1' => $data,
			'prefix2' => $data,
		);
		$this->assertEqual($expected, Stats::inc('multiBucket', $data, array('prefix1', 'prefix2')));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:multiBucket"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:prefix1:multiBucket"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:prefix2:multiBucket"));

		// multiple at once, with one bucket in array
		$expected = array('field1' => 1, 'field2' => 1);
		$this->assertEqual($expected, Stats::inc('withBucketArray', $expected, array('user' => 'foo')));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:withBucketArray"));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:user:foo:withBucketArray"));

		// multiple buckets as associated array
		$data = array('field1' => 1, 'field2' => 1);
		$expected = array(
			'user:foo' => $data,
			'year:2013' => $data,
		);
		$this->assertEqual($expected, Stats::inc('multiBucketArray', $data, array('user' => 'foo', 'year' => '2013')));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:multiBucketArray"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:user:foo:multiBucketArray"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:year:2013:multiBucketArray"));

		// multiple buckets as associated array with global
		$data = array('field1' => 2, 'field2' => 2);
		$expected = array(
			'global' => $data,
			'prefix1' => $data,
			'prefix2' => $data,
		);
		$result = Stats::inc('multiBucketWithGlobal',
			$data,
			array('global', 'prefix1', 'prefix2')
		);
		$this->assertEqual($expected, $result);
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:global:multiBucketWithGlobal"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:prefix1:multiBucketWithGlobal"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:prefix2:multiBucketWithGlobal"));

		$data = array('field1' => 3, 'field2' => 3);
		$expected = array(
			'global' => $data,
			'user:foo' => $data,
			'year:2013' => $data,
		);
		$result = Stats::inc('multiBucketArrayWithGlobal',
			$data,
			array('global', 'user' => 'foo', 'year' => '2013')
		);
		$this->assertEqual($expected, $result);
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:global:multiBucketArrayWithGlobal"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:user:foo:multiBucketArrayWithGlobal"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:year:2013:multiBucketArrayWithGlobal"));
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
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:foo"));
		$this->assertEqual($data2, $this->redis->hGetAll("$scope:stats:prefix1:foo"));
		$this->assertEqual($data3, $this->redis->hGetAll("$scope:stats:prefix2:foo"));
	}

	function testLength() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$data1 = array('one' => 14, 'two' => 22);
		$data2 = array('one' => 35, 'two' => 31, 'three' => 32);
		$data3 = array('one' => 28, 'two' => 16, 'three' => 12, 'four' => 34);
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
		$this->assertEqual(2, Stats::length('foo'));

		// one bucket
		$this->assertEqual(2, Stats::length('foo', 'global'));

		// one bucket as array
		$this->assertEqual(3, Stats::length('foo', array('prefix1')));

		// multi buckets as array
		$expected = array('global' => 2, 'prefix2' => 4);
		$this->assertEqual($expected, Stats::length('foo', array('global', 'prefix2')));
	}

	function testSet() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));

		// simplest call
		$expected = array('bar' => 'baz');
		$this->assertEqual($expected, Stats::set('foo', array('bar' => 'baz')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));

		// simple call, adding second field
		$expected = array('bar' => 'baz', 'baz' => 'bak');
		$this->assertEqual($expected, Stats::set('foo', array('baz' => 'bak')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));

		// simple call, overwrite second value
		$expected = array('bar' => 'baz', 'baz' => 'bam');
		$this->assertEqual($expected, Stats::set('foo', array('baz' => 'bam')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:foo"));

		// multiple at once
		$expected = array('field1' => 1, 'field2' => 1);
		$this->assertEqual($expected, Stats::set('multi', array('field1' => 1, 'field2' => 1)));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:multi"));

		// overwriting multiple at once
		$expected = array('field1' => 1, 'field2' => 2);
		$this->assertEqual($expected, Stats::set('multi', array('field1' => 1, 'field2' => 2)));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:global:multi"));

		// multiple at once, with plain bucket
		$expected = array('field1' => 1, 'field2' => 1);
		$this->assertEqual($expected, Stats::set('withBucket', $expected, 'prefix'));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:withBucket"));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:prefix:withBucket"));

		// multiple at once, with plain bucket, again (no inc?)
		$expected = array('field1' => 1, 'field2' => 1);
		$this->assertEqual($expected, Stats::set('withBucket', $expected, 'prefix'));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:withBucket"));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:prefix:withBucket"));

		// multiple buckets at once
		$data = array('field1' => 1, 'field2' => 1);
		$expected = array(
			'prefix1' => $data,
			'prefix2' => $data,
		);
		$this->assertEqual($expected, Stats::set('multiBucket', $data, array('prefix1', 'prefix2')));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:multiBucket"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:prefix1:multiBucket"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:prefix2:multiBucket"));

		// multiple at once, with one bucket in array
		$expected = array('field1' => 2, 'field2' => 2);
		$this->assertEqual($expected, Stats::set('withBucketArray', $expected, array('user' => 'foo')));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:withBucketArray"));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:stats:user:foo:withBucketArray"));

		// multiple buckets as associated array
		$data = array('field1' => 5, 'field2' => 'bla');
		$expected = array(
			'user:foo' => $data,
			'year:2013' => $data,
		);
		$this->assertEqual($expected, Stats::set('multiBucketArray', $data, array('user' => 'foo', 'year' => '2013')));
		$this->assertEqual(array(), $this->redis->hGetAll("$scope:stats:global:multiBucketArray"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:user:foo:multiBucketArray"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:year:2013:multiBucketArray"));

		// multiple buckets as associated array with global
		$data = array('field1' => 'foo', 'field2' => 'foo');
		$expected = array(
			'global' => $data,
			'prefix1' => $data,
			'prefix2' => $data,
		);
		$this->assertEqual($expected, Stats::set('multiBucket', $data, array('global', 'prefix1', 'prefix2')));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:global:multiBucket"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:prefix1:multiBucket"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:prefix2:multiBucket"));

		// multiple buckets as associated array with global
		$data = array('field1' => 'foobar', 'field2' => 'foobar');
		$expected = array(
			'global' => $data,
			'user:foo' => $data,
			'year:2013' => $data,
		);
		$result = Stats::set('multiBucket',
			array('field1' => 'foobar', 'field2' => 'foobar'),
			array('global', 'user' => 'foo', 'year' => '2013')
		);
		$this->assertEqual($expected, $result);
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:global:multiBucket"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:user:foo:multiBucket"));
		$this->assertEqual($data, $this->redis->hGetAll("$scope:stats:year:2013:multiBucket"));
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
		$this->assertEqual(array_values($data1), Stats::values('foo'));

		// with one prefix
		$this->assertEqual(array_values($data2), Stats::values('foo', 'prefix1'));

		// with one prefix as array
		$this->assertEqual(array_values($data3), Stats::values('foo', array('prefix2')));

		// with two prefixes as flat array
		$expected = array(
			'global' => array_values($data1),
			'prefix2' => array_values($data3),
		);
		$this->assertEqual($expected, Stats::values('foo', array('global', 'prefix2')));
	}

	function testSum() {
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
		$this->assertEqual(array_sum(array_values($data1)), Stats::sum('foo'));

		// with one prefix
		$this->assertEqual(array_sum(array_values($data2)), Stats::sum('foo', 'prefix1'));

		// with one prefix as array
		$this->assertEqual(array_sum(array_values($data3)), Stats::sum('foo', array('prefix2')));

		// with two prefixes as flat array
		$expected = array(
			'global' => array_sum(array_values($data1)),
			'prefix2' => array_sum(array_values($data3)),
		);
		$this->assertEqual($expected, Stats::sum('foo', array('global', 'prefix2')));
	}
}
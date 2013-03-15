<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\tests\cases\storage;

use li3_redis\storage\Redis;
use li3_redis\storage\Lists;

use lithium\util\Collection;
use Redis as RedisCore;

class ListsTest extends \lithium\test\Unit {

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

	function testAdd() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));

		// simplest call
		$expected = array('bar');
		$this->assertEqual(1, Lists::add('foo', 'bar'));
		$this->assertEqual($expected, $this->redis->lRange("$scope:lists:foo", 0, -1));

		$expected = array('bar', 'baz');
		$this->assertEqual(2, Lists::add('foo', 'baz'));
		$this->assertEqual($expected, $this->redis->lRange("$scope:lists:foo", 0, -1));

		// call with array
		$expected = array('bar', 'baz');
		$this->assertEqual(2, Lists::add('withArray', array('bar', 'baz')));
		$this->assertEqual($expected, $this->redis->lRange("$scope:lists:withArray", 0, -1));

		// call with big array
		$expected = array_fill(0, 100, 'blub');
		$this->assertEqual(100, Lists::add('manyItems', $expected));
		$this->assertEqual($expected, $this->redis->lRange("$scope:lists:manyItems", 0, -1));

		// call with bigger array
		$expected = array_fill(0, 1000, 'blub');
		$this->assertEqual(1000, Lists::add('lotsItems', $expected));
		$this->assertEqual($expected, $this->redis->lRange("$scope:lists:lotsItems", 0, -1));

		// call with collection
		$data = array('lassy', 'barker', 'wuff', 'patty');
		$dogs = new Collection(compact('data'));
		$expected = $dogs->to('array');
		$this->assertEqual(4, Lists::add('dogs', $dogs));
		$this->assertEqual($expected, $this->redis->lRange("$scope:lists:dogs", 0, -1));

		// offset
		$this->assertEqual(array('barker'), $this->redis->lRange("$scope:lists:dogs", 1, 1));
	}

	function testGet() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));

		$this->redis->rPush("$scope:lists:foo", 'bar');
		$this->redis->rPush("$scope:lists:foo", 'baz');
		$this->redis->rPush("$scope:lists:foo", 'bam');
		$data = array('bar', 'baz', 'bam');
		$expected = new Collection(compact('data'));

		// simple
		$this->assertEqual($expected, Lists::get('foo'));

		// simple with index
		$this->assertEqual($expected, Lists::get('foo', array(0, -1)));
		$this->assertEqual($expected, Lists::get('foo', array(0)));

		// multi
		$this->redis->rPush("$scope:lists:a:multi", 'bar');
		$this->redis->rPush("$scope:lists:a:multi", 'baz');
		$this->redis->rPush("$scope:lists:a:multi", 'bam');
		$this->redis->rPush("$scope:lists:b:multi", 'bar');
		$this->redis->rPush("$scope:lists:b:multi", 'baz');
		$this->redis->rPush("$scope:lists:b:multi", 'bam');
		$expected = array('a' => array('bar', 'baz', 'bam'), 'b' => array('bar', 'baz', 'bam'));

		$result = Lists::get('multi', array(), array('a', 'b'), array('merge' => false));
		$this->assertEqual($expected, $result->to('array'));

		// merge multi
		$expected = array('bar', 'baz', 'bam', 'bar', 'baz', 'bam');
		$result = Lists::get('multi', array(), array('a', 'b'));
		$this->assertEqual($expected, $result->to('array'));
		$result = Lists::get('multi', array(), array('a', 'b'), array('merge' => true));
		$this->assertEqual($expected, $result->to('array'));

		// offset
		$this->redis->rPush("$scope:lists:dogs", 'lassy');
		$this->redis->rPush("$scope:lists:dogs", 'barker');
		$this->redis->rPush("$scope:lists:dogs", 'wuff');
		$this->redis->rPush("$scope:lists:dogs", 'patty');

		$options = array('collect' => false);
		$this->assertEqual(array('barker'), Lists::get('dogs', array(1, 1), null, $options));
		$this->assertEqual(array('barker', 'wuff'), Lists::get('dogs', array(1, 2), null, $options));
		$this->assertEqual(array('barker', 'wuff', 'patty'), Lists::get('dogs', array(1, 3), null, $options));
		$this->assertEqual(array('barker', 'wuff', 'patty'), Lists::get('dogs', array(1, -1), null, $options));
		$this->assertEqual(array('wuff', 'patty'), Lists::get('dogs', array(2, 3), null, $options));
		$this->assertEqual(array('patty'), Lists::get('dogs', array(-1, -1), null, $options));
		$this->assertEqual(array('patty'), Lists::get('dogs', array(-1), null, $options));

	}

	function testSet() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));

		$this->redis->rPush("$scope:lists:foo", 'bar');
		$this->redis->rPush("$scope:lists:foo", 'baz');
		$this->redis->rPush("$scope:lists:foo", 'bam');
		$this->assertEqual(3, $this->redis->lLen("$scope:lists:foo"));

		$this->assertTrue(Lists::set('foo', 1, 'ball'));
		$this->assertEqual('ball', $this->redis->lGet("$scope:lists:foo", 1));
		$this->assertTrue(Lists::set('foo', 2, 'barry'));
		$this->assertEqual('barry', $this->redis->lGet("$scope:lists:foo", 2));

		$this->redis->rPush("$scope:lists:dogs", 'lassy');
		$this->redis->rPush("$scope:lists:dogs", 'barker');
		$this->redis->rPush("$scope:lists:dogs", 'wuff');
		$this->redis->rPush("$scope:lists:dogs", 'patty');
		$this->assertEqual(4, $this->redis->lLen("$scope:lists:dogs"));

		$expected = array(0 => true, 3 => true);
		$this->assertEqual($expected, Lists::set('dogs', array(0 => 'judy', 3 => 'barry')));
		$this->assertEqual('judy', $this->redis->lGet("$scope:lists:dogs", 0));
		$this->assertEqual('barker', $this->redis->lGet("$scope:lists:dogs", 1));
		$this->assertEqual('wuff', $this->redis->lGet("$scope:lists:dogs", 2));
		$this->assertEqual('barry', $this->redis->lGet("$scope:lists:dogs", 3));
	}

	function testPop() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));

		$this->redis->rPush("$scope:lists:foo", 'bar');
		$this->redis->rPush("$scope:lists:foo", 'baz');
		$this->redis->rPush("$scope:lists:foo", 'bam');
		$this->assertEqual(3, $this->redis->lLen("$scope:lists:foo"));
		$this->assertEqual('bar', Lists::pop('foo'));
		$this->assertEqual(2, $this->redis->lLen("$scope:lists:foo"));
		$this->assertEqual('baz', Lists::pop('foo'));
		$this->assertEqual(1, $this->redis->lLen("$scope:lists:foo"));
		$this->assertEqual('bam', Lists::pop('foo'));
		$this->assertEqual(0, $this->redis->lLen("$scope:lists:foo"));

		$this->redis->rPush("$scope:lists:otherDirection", 'uno');
		$this->redis->rPush("$scope:lists:otherDirection", 'duo');
		$this->redis->rPush("$scope:lists:otherDirection", 'tre');
		$this->assertEqual(3, $this->redis->lLen("$scope:lists:otherDirection"));
		$this->assertEqual('tre', Lists::pop('otherDirection', false, array('last' => true)));
		$this->assertEqual(2, $this->redis->lLen("$scope:lists:otherDirection"));
		$this->assertEqual('duo', Lists::pop('otherDirection', false, array('last' => true)));
		$this->assertEqual(1, $this->redis->lLen("$scope:lists:otherDirection"));
		$this->assertEqual('uno', Lists::pop('otherDirection', false, array('last' => true)));
		$this->assertEqual(0, $this->redis->lLen("$scope:lists:otherDirection"));
		$this->assertFalse(Lists::pop('otherDirection', false, array('last' => true)));
		$this->assertEqual(0, $this->redis->lLen("$scope:lists:otherDirection"));

		$this->redis->rPush("$scope:lists:otherDirection2", 'uno');
		$this->redis->rPush("$scope:lists:otherDirection2", 'duo');
		$this->redis->rPush("$scope:lists:otherDirection2", 'tre');
		$this->assertEqual(3, $this->redis->lLen("$scope:lists:otherDirection2"));
		$this->assertEqual('tre', Lists::popLast('otherDirection2'));
		$this->assertEqual(2, $this->redis->lLen("$scope:lists:otherDirection2"));
		$this->assertEqual('duo', Lists::popLast('otherDirection2'));
		$this->assertEqual(1, $this->redis->lLen("$scope:lists:otherDirection2"));
		$this->assertEqual('uno', Lists::popLast('otherDirection2'));
		$this->assertEqual(0, $this->redis->lLen("$scope:lists:otherDirection2"));
	}

	function testCount() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));

		$this->redis->rPush("$scope:lists:foo", 'bar');
		$this->redis->rPush("$scope:lists:foo", 'baz');
		$this->assertEqual(2, Lists::count('foo'));

		$this->redis->rPush("$scope:lists:foo", 'bam');
		$this->assertEqual(3, Lists::count('foo'));
		$this->assertEqual(3, Lists::size('foo'));
		$this->assertEqual(3, Lists::length('foo'));
	}

}


?>
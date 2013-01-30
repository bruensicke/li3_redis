<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\tests\cases\storage;

use li3_redis\storage\Redis;

use Redis as RedisCore;

class RedisTest extends \lithium\test\Unit {

	public $redis;

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

	function testWrite() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$expected = 'bar';
		$this->assertEqual($expected, Redis::write('foo', 'bar'));
		$this->assertEqual($expected, $this->redis->get("$scope:foo"));
		$expected = 'baz';
		$this->assertEqual($expected, Redis::write('foo', 'baz'));
		$this->assertEqual($expected, $this->redis->get("$scope:foo"));
		$expected = array('bar' => 'baz');
		$this->assertEqual($expected, Redis::write('isHash', array('bar' => 'baz')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:isHash"));
		$expected = array('field1' => 'val1', 'field2' => 'val2');
		$this->assertEqual($expected, Redis::write('isHash2', array('field1' => 'val1', 'field2' => 'val2')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:isHash2"));
		$expected = array('bar' => 'baz');
		$this->assertEqual($expected, Redis::write(array('bar' => 'baz')));
		$this->assertEqual('baz', $this->redis->get("$scope:bar"));
		$expected = array('key1' => 'val1', 'key2' => 'val2');
		$this->assertEqual($expected, Redis::write(array('key1' => 'val1', 'key2' => 'val2')));
		$this->assertEqual('val1', $this->redis->get("$scope:key1"));
		$this->assertEqual('val2', $this->redis->get("$scope:key2"));
		$expected = array('key1' => true, 'key2' => false);
		$this->assertEqual($expected, Redis::write(array('key1' => true, 'key2' => false)));
		$this->assertTrue($this->redis->get("$scope:key1"));
		$this->assertFalse($this->redis->get("$scope:key2"));
	}

	function testIncrementHash() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$expected = array('bar' => '1', 'baz' => '2');
		$this->assertEqual($expected, Redis::incrementHash('foo', array('bar' => 1, 'baz' => 2)));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:foo"));
		$expected = array('bar' => 1, 'baz' => 2, 'uncountable' => 'foo');
		$this->assertEqual($expected, Redis::incrementHash('withString', array('bar' => 1, 'baz' => 2, 'uncountable' => 'foo')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:withString"));
		$expected = array('bar' => 2, 'baz' => 4, 'uncountable' => 'foo');
		$this->assertEqual($expected, Redis::incrementHash('withString', array('bar' => 1, 'baz' => 2, 'uncountable' => 'foo')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:withString"));
		$this->assertEqual(1, Redis::incrementHash('onlyString', 'count'));
		$this->assertEqual(array('count' => 1), $this->redis->hGetAll("$scope:onlyString"));
		$this->assertEqual(2, Redis::incrementHash('onlyString', 'count'));
		$this->assertEqual(array('count' => 2), $this->redis->hGetAll("$scope:onlyString"));
	}

	function testWriteHash() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$expected = array('name' => 'Joe', 'salary' => '2000');
		$this->assertEqual($expected, Redis::writeHash('foo', array('name' => 'Joe', 'salary' => '2000')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:foo"));
		$expected = array('name' => 'John', 'title' => 'manager', 'salary' => '2000');
		$this->assertEqual($expected, Redis::writeHash('foo', array('name' => 'John', 'title' => 'manager')));
		$this->assertEqual($expected, $this->redis->hGetAll("$scope:foo"));
	}

	function testRead() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$this->assertTrue($this->redis->hMset("$scope:foo", array('name' => 'Joe', 'salary' => 2000)));
		$this->assertTrue($this->redis->hExists("$scope:foo", 'name'));
		$this->assertTrue($this->redis->hExists("$scope:foo", 'salary'));
		$this->assertTrue($this->redis->set("$scope:bar", 'baz'));
		$this->assertTrue($this->redis->set("$scope:baz", 'foobar'));
		$expected = array('name' => 'Joe', 'salary' => '2000');
		$this->assertEqual($expected, Redis::read('foo'));
		$expected = 'baz';
		$this->assertEqual($expected, Redis::read('bar'));
		$expected = 'foobar';
		$this->assertEqual($expected, Redis::read('baz'));
		$expected = array('bar' => 'baz', 'baz' => 'foobar');
		$this->assertEqual($expected, Redis::read(array('bar', 'baz')));
		$expected = array('bar' => 'baz', 'baz' => 'foobar', 'foo' => array('name' => 'Joe', 'salary' => '2000'));
		$this->assertEqual($expected, Redis::read(array('bar', 'baz', 'foo')));
	}

	function testReadHash() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$this->assertTrue($this->redis->hMset("$scope:foo", array('name' => 'Joe', 'salary' => 2000)));
		$this->assertTrue($this->redis->hExists("$scope:foo", 'name'));
		$this->assertTrue($this->redis->hExists("$scope:foo", 'salary'));
		$expected = 'Joe';
		$this->assertEqual($expected, Redis::readHash('foo', 'name'));
		$expected = 'Joe';
		$this->assertEqual($expected, Redis::readHash('foo', array('name')));
		$expected = '2000';
		$this->assertEqual($expected, Redis::readHash('foo', 'salary'));
		$expected = array('name' => 'Joe', 'salary' => '2000');
		$this->assertEqual($expected, Redis::readHash('foo', array('name', 'salary')));
		$expected = array('name' => 'Joe', 'salary' => '2000', 'non-existent' => false);
		$this->assertEqual($expected, Redis::readHash('foo', array('name', 'salary', 'non-existent')));
		$expected = array('name' => 'Joe', 'salary' => '2000');
		$this->assertEqual($expected, Redis::readHash('foo'));
	}

	function testDelete() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$this->assertEqual('test', $this->redis->set("$scope:bar", 'test'));
		$this->assertTrue(Redis::delete('bar'));
		$this->assertEqual(1, $this->redis->incr("$scope:baz"));
		$this->assertTrue(Redis::delete('baz'));
		$this->assertFalse(Redis::delete('non-existent'));
	}

	function testDeleteFromHash() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$this->assertTrue($this->redis->hMset("$scope:foo", array('name' => 'Joe', 'salary' => 2000)));
		$this->assertTrue($this->redis->hExists("$scope:foo", 'name'));
		$this->assertTrue($this->redis->hExists("$scope:foo", 'salary'));
		$this->assertTrue(Redis::deleteFromHash('foo', 'name'));
		$this->assertFalse($this->redis->hExists("$scope:foo", 'name'));
		$this->assertTrue($this->redis->hExists("$scope:foo", 'salary'));
		$this->assertTrue($this->redis->hMset("$scope:foo", array('name' => 'Joe', 'salary' => 2000)));
		$expected = array('name' => true, 'salary' => true);
		$this->assertEqual($expected, Redis::deleteFromHash('foo', array('name', 'salary')));
		$this->assertFalse($this->redis->hExists("$scope:foo", 'name'));
		$this->assertFalse($this->redis->hExists("$scope:foo", 'salary'));
		$this->assertTrue($this->redis->hMset("$scope:foo", array('name' => 'Joe', 'salary' => 2000)));
		$expected = array('name' => true, 'salary' => true, 'non-existent' => false);
		$this->assertEqual($expected, Redis::deleteFromHash('foo', array('name', 'salary', 'non-existent')));
		$this->assertFalse($this->redis->hExists("$scope:foo", 'name'));
		$this->assertFalse($this->redis->hExists("$scope:foo", 'salary'));
		$this->assertFalse($this->redis->hExists("$scope:foo", 'non-existent'));
	}

	function testIncrement() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$this->assertEqual(1, Redis::increment('foo'));
		$this->assertEqual(1, $this->redis->get("$scope:foo"));
		$this->assertEqual(5, Redis::increment('bar', 5));
		$this->assertEqual(5, $this->redis->get("$scope:bar"));
		$this->assertEqual(2, Redis::increment('foo'));
		$this->assertEqual(2, $this->redis->get("$scope:foo"));
		$this->assertEqual(1, Redis::increment('foo', 1, array('format' => $scope.':test')));
		$this->assertEqual(1, $this->redis->get("$scope:test:foo"));
		$this->assertEqual(1, Redis::increment('foo', 1, array('namespace' => 'bar')));
		$this->assertEqual(1, $this->redis->get("$scope:bar:foo"));
		$this->assertEqual(1, Redis::increment('foo', 1, array('namespace' => 'bar', 'prefix' => 'baz')));
		$this->assertEqual(1, $this->redis->get("$scope:bar:baz:foo"));
	}

	function testDecrement() {
		$scope = __FUNCTION__;
		Redis::config(array('format' => $scope));
		$this->assertEqual(1, $this->redis->incr("$scope:foo"));
		$this->assertEqual(0, Redis::decrement('foo'));
		$this->assertEqual(-1, Redis::decrement('foo'));
		$this->assertEqual(-1, $this->redis->get("$scope:foo"));
		$this->assertEqual(-5, Redis::decrement('bar', 5));
		$this->assertEqual(-5, $this->redis->get("$scope:bar"));
		$this->assertEqual(-2, Redis::decrement('foo'));
		$this->assertEqual(-2, $this->redis->get("$scope:foo"));
		$this->assertEqual(-1, Redis::decrement('foo', 1, array('format' => $scope.':test')));
		$this->assertEqual(-1, $this->redis->get("$scope:test:foo"));
		$this->assertEqual(-1, Redis::decrement('foo', 1, array('namespace' => 'bar')));
		$this->assertEqual(-1, $this->redis->get("$scope:bar:foo"));
		$this->assertEqual(-1, Redis::decrement('foo', 1, array('namespace' => 'bar', 'prefix' => 'baz')));
		$this->assertEqual(-1, $this->redis->get("$scope:bar:baz:foo"));
	}

	function testAddPrefix() {
		$expected = 'bar:foo';
		$this->assertEqual($expected, Redis::addPrefix('foo', 'bar'));
		$expected = 'foo:bar';
		$this->assertEqual($expected, Redis::addPrefix('bar', 'foo'));
		$expected = 'foo!bar';
		$this->assertEqual($expected, Redis::addPrefix('bar', 'foo', array('separator' => '!')));
		$expected = 'fooHELPbar';
		$this->assertEqual($expected, Redis::addPrefix('bar', 'foo', array('separator' => 'HELP')));
		$expected = 'foo.bar';
		$this->assertEqual($expected, Redis::addPrefix('bar', 'foo', array('separator' => '.')));
		$expected = 'bar';
		$this->assertEqual($expected, Redis::addPrefix('bar', '', array('separator' => '!')));
		$expected = 'foo';
		$this->assertEqual($expected, Redis::addPrefix('foo'));
		$expected = 'foo';
		$this->assertEqual($expected, Redis::addPrefix('', 'foo'));
	}

	function testResolveFormat() {
		$expected = '';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => '')));
		$expected = '';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => '{:key}')));
		$expected = 'test';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => '{:environment}')));
		$expected = 'test';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => '{:environment}:{:key}')));
		$expected = 'test';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => '{:environment}.{:key}', 'separator' => '.')));
		$expected = 'test';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => '{:environment}!{:key}', 'separator' => '!')));
		$expected = 'test.';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => '{:environment}.{:key}')));
		$expected = 'test!';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => '{:environment}!{:key}')));
		$expected = 'test:foo';
		$this->assertEqual($expected, Redis::resolveFormat('foo', array('format' => '{:environment}')));
		$expected = 'test:foo';
		$this->assertEqual($expected, Redis::resolveFormat('foo', array('format' => '{:environment}:{:key}')));
		$expected = 'foo';
		$this->assertEqual($expected, Redis::resolveFormat('foo', array('format' => '{:key}')));
		$expected = 'app.test.foo';
		$this->assertEqual($expected, Redis::resolveFormat('foo', array('format' => 'app.{:environment}.{:key}')));
		$expected = 'app:test:foo';
		$this->assertEqual($expected, Redis::resolveFormat('foo', array('format' => 'app:{:environment}:{:key}')));
		$expected = 'app:test';
		$this->assertEqual($expected, Redis::resolveFormat('', array('format' => 'app:{:environment}:{:key}')));
		$expected = 'app:test';
		$this->assertEqual($expected, Redis::resolveFormat(false, array('format' => 'app:{:environment}:{:key}')));
		$expected = 'app:test';
		$this->assertEqual($expected, Redis::resolveFormat(null, array('format' => 'app:{:environment}:{:key}')));
		$expected = 'app:test:2';
		$this->assertEqual($expected, Redis::resolveFormat(2, array('format' => 'app:{:environment}:{:key}')));
		$expected = 'foo:test:bar';
		$this->assertEqual($expected, Redis::resolveFormat('bar', array(
			'format' => '{:app}:{:environment}:{:key}',
			'replacements' => array(
				'{:app}' => 'foo',
		))));
	}

	function testCleanKeys() {
		$expected = 'bar';
		$this->assertEqual($expected, Redis::cleanKeys('app:test:foo', 'app:test:foo:bar'));
		$expected = 'bar:baz';
		$this->assertEqual($expected, Redis::cleanKeys('app:test:foo', 'app:test:foo:bar:baz'));
		$expected = 'app:test:foo:value';
		$this->assertEqual($expected, Redis::cleanKeys('app:test:foo', 'app:test:foo:value', array('raw' => true)));
		$expected = array('foo:bar:baz' => 1, 'foo:bar:bam' => 22);
		$data = array(
			'app:test:foo:bar:baz' => 1,
			'app:test:foo:bar:bam' => 22,
		);
		$this->assertEqual($expected, Redis::cleanKeys('app:test', $data));
	}

	function testGetKey() {
		$expected = 'foo';
		$this->assertEqual($expected, Redis::getKey('foo', array('format' => '')));
		$expected = 'app:test:foo:bar';
		$this->assertEqual($expected, Redis::getKey('bar', array('format' => 'app:test:foo')));
		$expected = 'app:test:foo';
		$this->assertEqual($expected, Redis::getKey('foo', array('format' => 'app:{:environment}')));
		$expected = 'app|test|foo';
		$this->assertEqual($expected, Redis::getKey('foo', array('format' => 'app|{:environment}', 'separator' => '|')));
		$expected = 'app:test:foo';
		$this->assertEqual($expected, Redis::getKey('foo', array('format' => 'app:{:environment}:{:key}')));
		$expected = 'app:test:foo:bar';
		$this->assertEqual($expected, Redis::getKey('bar', array('format' => 'app:{:environment}:{:key}', 'prefix' => 'foo')));
		$expected = 'foo:bar';
		$this->assertEqual($expected, Redis::getKey('bar', array('format' => '{:key}', 'prefix' => 'foo')));
		$expected = 'foo:bar';
		$this->assertEqual($expected, Redis::getKey('bar', array('format' => '', 'prefix' => 'foo')));
		$expected = '';
		$this->assertEqual($expected, Redis::getKey('', array('format' => '', 'prefix' => '')));
		$expected = 'foo';
		$this->assertEqual($expected, Redis::getKey('foo', array('format' => '', 'prefix' => '')));
		$expected = 'foo';
		$this->assertEqual($expected, Redis::getKey('foo', array('format' => '')));
		$expected = 'foo:bar';
		$this->assertEqual($expected, Redis::getKey('bar', array('format' => '', 'prefix' => '', 'namespace' => 'foo')));
		$expected = 'foo:bar:baz';
		$this->assertEqual($expected, Redis::getKey('baz', array('format' => '', 'prefix' => 'bar', 'namespace' => 'foo')));
	}

	function testGetKeyArray() {
		$expected = array('app:foo', 'app:bar');
		$this->assertEqual($expected, Redis::getKey(array('foo', 'bar'), array('format' => 'app')));
	}


}

?>
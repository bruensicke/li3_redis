<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\tests\cases\extensions\data\source;

use li3_redis\extensions\data\source\Redis;
use lithium\data\Connections;
use lithium\analysis\Inspector;

use ReflectionObject;
/**
 *
 */
class RedisTest extends \lithium\test\Unit {

	public function skip() {
		$this->skipIf(!Redis::enabled(), 'The Redis extension is not loaded!');
	}

	public function testEnabled() {
		$this->assertTrue(Redis::enabled());
		$this->assertTrue(Redis::enabled('arrays'));
		$this->assertTrue(Redis::enabled('transactions'));
		$this->assertFalse(Redis::enabled('relationships'));
		$this->assertFalse(Redis::enabled('schema'));
		$this->assertFalse(Redis::enabled('booleans'));
	}

	public function testDefaults() {
		$expected = array(
			'init'             => false,
			'host'             => 'localhost',
			'port'             => '6379',
			'timeout'          => 3,
			'retry_interval'   => 100,
			'persistent'       => false,
			'persistent_id'    => null,
			'retries'          => null,
			'password'         => null,
			'database'         => null,
			'lazy_connect'     => true,
			'autoConnect'      => true,
		);
		$redis = new Redis(array('init' => false));
		$reflection = new ReflectionObject($redis);
		$configProperty = $reflection->getProperty('_config');
		$configProperty->setAccessible(true);
		$result = $configProperty->getValue($redis);
		$this->assertEqual($expected, $result);
	}

	public function testConnect() {
		$result = new Redis(array());
		$this->assertTrue($result->isConnected());
		$this->assertTrue(is_array($result->connection->config('GET', '*')));
		$this->assertTrue(is_array($result->connection->info()));
		$result = new Redis;
		$this->assertTrue($result->isConnected());
		$this->assertTrue(is_array($result->connection->config('GET', '*')));
		$this->assertTrue(is_array($result->connection->info()));
	}

	public function testDisconnect() {
		$redis = new Redis(array());
		$this->assertTrue($redis->isConnected());
		$this->assertTrue($redis->disconnect());
		$this->assertFalse($redis->isConnected());
	}
}

?>
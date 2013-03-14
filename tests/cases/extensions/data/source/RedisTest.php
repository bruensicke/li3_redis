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

	/**
	 * Connection configuration.
	 */
	protected $_connectionConfig = array();

	/**
	 * Connection to the database.
	 */
	public $connection = null;

	/**
	 * Skip the test if a Redis adapter configuration is unavailable.
	 */
	// public function skip() {
	// 	$this->skipIf(!Redis::enabled(), 'The Redis extension is not loaded!');

	// 	$this->_connectionConfig = Connections::get('li3_redis', array('config' => true));
	// 	$hasDb = (isset($this->_connectionConfig['type']) && $this->_connectionConfig['type'] == 'Redis');
	// 	$message = 'Test database is either unavailable, or not a Redis connection!';
	// 	$this->skipIf(!$hasDb, $message);

	// 	$this->connection = new Redis($this->_connectionConfig);
	// 	$this->connection->select(1);
	// }

	// public function testEnabled() {
	// 	$this->assertTrue(Redis::enabled());
	// 	$this->assertTrue(Redis::enabled('transactions'));
	// 	$this->assertFalse(Redis::enabled('relations'));
	// }

	public function testDefaults() {
		$expected = array(
			'init'       => false,
			'host'       => 'localhost',
			'port'       => '6379',
			'timeout'    => '3',
			'password'   => null,
			'database'   => null,
			'persistent' => false,
			'autoConnect' => true,
		);
		$redis = new Redis(array('init' => false));
		$reflection = new ReflectionObject($redis);
		$configProperty = $reflection->getProperty('_config');
		$configProperty->setAccessible(true);
		$result = $configProperty->getValue($redis);
		$this->assertEqual($expected, $result);
	}

	public function testConnect() {
		$result = new Redis($this->_connectionConfig);
		$this->assertTrue($result->isConnected());
		$this->assertTrue(is_array($result->connection->config('GET', '*')));
		$this->assertTrue(is_array($result->connection->info()));
	}

	public function testDisconnect() {
		$redis = new Redis($this->_connectionConfig);
		$this->assertTrue($redis->isConnected());
		$this->assertTrue($redis->disconnect());
		$this->assertFalse($redis->isConnected());
	}
}

?>
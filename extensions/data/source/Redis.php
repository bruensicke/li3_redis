<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\extensions\data\source;

use Exception;
use Redis as RedisCore;
use lithium\core\NetworkException;

/**
 * A data source adapter which allows you to connect to the Redis database engine.
 */
class Redis extends \lithium\data\Source {

	/**
	 *
	 */
	public function __construct(array $config = array()) {
		$defaults = array(
			'host'       => 'localhost',
			'port'       => '6379',
			'timeout'    => '3',
			'password'   => null,
			'database'   => null,
			'persistent' => false
		);
		parent::__construct($config + $defaults);
	}

	/**
	 * With no parameter, checks to see if the `redis` extension is installed. With a
	 * parameter, queries for a specific supported feature.
	 *
	 * @param string $feature Test for support for a specific feature, i.e. `"arrays"`.
	 * @return boolean Returns `true` if the particular feature support is enabled (or `false`).
	 */
	public static function enabled($feature = null) {
		if (!$feature) {
			return extension_loaded('redis');
		}
		$features = array(
			'arrays' => true,
			'transactions' => true,
			'booleans' => true,
			'relationships' => false
		);
		return isset($features[$feature]) ? $features[$feature] : null;
	}

	/**
	 *
	 */
	public function connect() {
		$config = $this->_config;
		$this->_isConnected = false;

		if (stristr(':', $config['host'])) {
			list($host, $port) = explode(':', $config['host']);
		} else {
			$host = $config['host'];
			$port = $config['port'];
		}
		$method = $config['persistent'] ? 'pconnect' : 'connect';

		try {
			$this->connection = new RedisCore();
			if (!empty($config['password'])) {
				$this->connection->auth($config['password']);
			}
			$this->connection->$method($host, $port, $config['timeout']);
			if (!empty($config['database'])) {
				$this->connection->select($config['database']);
			}
		} catch(Exception $e) {
			throw new NetworkException("Could not connect to the database: " . $e->getMessage(), 503);
			return false;
		}

		return $this->_isConnected = true;
	}

	/**
	 * Dispatches a not-found method to the Redis connection object.
	 *
	 * If you want to know, what methods are available, have a look at the readme of phprdis.
	 * One use-case might be to query possible keys, e.g.
	 *
	 * {{{Connections::get('li3_redis')->keys('*');}}}
	 *
	 * @link https://github.com/nicolasff/phpredis GitHub: PhpRedis Extension
	 * @param string $method Name of the method to call
	 * @param array $params Parameter list to use when calling $method
	 * @return mixed Returns the result of the method call
	 */
	public function __call($method, $params = array()) {
		return call_user_func_array(array(&$this->connection, $method), $params);
	}

	/**
	 *
	 */
	public function disconnect() {
		$this->_isConnected = false;
		unset($this->connection);
		return true;
	}

	/**
	 *
	 */
	public function sources($class = null) {}

	/**
	 *
	 */
	public function describe($entity, $schema = array(), array $meta = array()){}

	/**
	 *
	 */
	public function relationship($class, $type, $name, array $options = array()) {}

	/**
	 *
	 */
	public function create($query, array $options = array()) {}

	/**
	 *
	 */
	public function read($query, array $options = array()) {}

	/**
	 *
	 */
	public function update($query, array $options = array()) {}

	/**
	 *
	 */
	public function delete($query, array $options = array()) {}

}

?>
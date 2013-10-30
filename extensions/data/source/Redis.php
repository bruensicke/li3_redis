<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\extensions\data\source;

use RedisArray;
use Redis as RedisCore;
use RedisException;
use lithium\core\NetworkException;

use ReflectionExtension;

/**
 * A data source adapter which allows you to connect to the Redis database engine.
 *
 * It allows connecting to a RedisCluster if you specify `host` param as array.
 * This has a lot of implications, you can read up here:
 * https://github.com/nicolasff/phpredis/blob/master/arrays.markdown#readme
 *
 * Please note: databases are not supported by RedisArray, so you have to work with
 * database 0, then.
 *
 * For convienience here is an example distributor function:
 *
 * function($key) use ($config) {
 *	  $number_of_hosts = count($config['host']);
 *	  $md5 = substr(md5($key), -4);
 *	  $hash_number = base_convert($md5, 16, 10);
 *	  return ($hash_number % $number_of_hosts);
 * }
 *
 */
class Redis extends \lithium\data\Source {

	/**
	 *
	 */
	public function __construct(array $config = array()) {
		$defaults = array(
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
			'booleans' => false,
			'relationships' => false,
			'schema' => false,
		);
		return isset($features[$feature]) ? $features[$feature] : null;
	}

	/**
	 *
	 */
	public function connect() {
		extract($this->_config);
		$this->_isConnected = false;

		$method = $persistent ? 'pconnect' : 'connect';

		try {
			if (is_array($host)) {
				$this->connection = new RedisArray($host, $this->_config);
				$r = $this->connection->_hosts();
			} else {
				$this->connection = new RedisCore;
				if (!empty($password)) {
					$this->connection->auth($password);
				}
				$extension = new ReflectionExtension('redis');
				$con = (version_compare($extension->getVersion(), '2.2.4') >= 0)
					? $this->connection->$method($host, $port, $timeout, $persistent_id, $retry_interval)
					: $this->connection->$method($host, $port, $timeout);
				if ($con) {
					return false;
				}
				if (!empty($database)) {
					$this->connection->select($database);
				}
			}

		} catch (RedisException $e) {
			throw new NetworkException("Could not connect to the database.", 503, $e);
		}
		return $this->_isConnected = true;
	}

	/**
	 * Dispatches a not-found method to the Redis connection object.
	 *
	 * If you want to know, what methods are available, have a look at the readme of phpredis.
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

	public function hGet($key, $hashKey) {
		return $this->connection->{__FUNCTION__}($key, $hashKey);
	}

	public function hGetAll($key) {
		return $this->connection->{__FUNCTION__}($key);
	}

	public function hIncrBy($key, $member, $value) {
		return $this->connection->{__FUNCTION__}($key, $member, $value);
	}

	public function hMset($key, $members) {
		return $this->connection->{__FUNCTION__}($key, $members);
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
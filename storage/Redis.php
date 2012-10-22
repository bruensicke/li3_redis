<?php

namespace li3_redis\storage;

use lithium\core\Libraries;
use lithium\core\Environment;
use lithium\data\Connections;
use lithium\util\String;
use lithium\util\Set;

class Redis extends \lithium\core\StaticObject {

	/**
	 * Stores configuration information for object instances at time of construction.
	 * **Do not override.** Pass any additional variables to `Redis::init()`.
	 *
	 * @var array
	 */
	protected static $_config = array();

	/**
	 * Redis Connection instance used by this class.
	 *
	 * @var object Redis object
	 */
	public static $connection;

	/**
	 * Sets default connection options.
	 *
	 * @see li3_redis\storage\Redis::config()
	 * @param array $options
	 * @return void
	 */
	public static function __init() {
		static::config();
	}

	/**
	 * Configures the redis backend for use.
	 *
	 * This method is called by `Redis::init()` and `Redis::__init()`.
	 *
	 * @param array $options Possible options are:
	 *     - `format`: allows setting a prefix for keys, i.e. Environment
	 *
	 * @return void
	 */
	public static function config(array $options = array()) {
		$config = Libraries::get('li3_redis');
		$defaults = array('connection' => 'li3_redis', 'format' => '{:environment}:{:name}');
		if (!empty($config['format'])) {
			$defaults['format'] = $config['format'];
		}
		if (!empty($config['connection'])) {
			$defaults['connection'] = $config['connection'];
		}
		$options += $defaults;
		static::connection(Connections::get($options['connection']));
		return static::$_config = $options;
	}

	/**
	 * returns a replaced version of a generic message format
	 *
	 * used to interpolate names/folders for keys
	 *
	 * @param string $name optional, if given, inserts the key
	 * @return string the parsed string
	 */
	public static function formatKey($name = null, $namespace = null) {
		$name = ($namespace) ? sprintf('%s:%s', $namespace, $name) : $name;
		return String::insert(static::$_config['format'], array(
			'name' => ($name) ? : '{:name}',
			'environment' => Environment::get(),
		));
	}

	public static function find($search = '*') {
		$params = compact('search');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			return call_user_func_array(array($self::connection(), 'keys'), array_values($params));
		});
	}

	public static function connection($connection = null) {
		if (!is_null($connection)) {
			static::$connection = $connection;
		}
		return static::$connection;
	}
















	/**
	 * Sets expiration time for cache keys
	 *
	 * @param string $key The key to uniquely identify the cached item
	 * @param mixed $expiry A `strtotime()`-compatible string indicating when the cached item
	 *              should expire, or a Unix timestamp.
	 * @return boolean Returns `true` if expiry could be set for the given key, `false` otherwise.
	 */
	protected function _ttl($key, $expiry) {
		return $this->connection->expireAt($key, is_int($expiry) ? $expiry : strtotime($expiry));
	}

	/**
	 * Write value(s) to the cache
	 *
	 * @param string $key The key to uniquely identify the cached item
	 * @param mixed $value The value to be cached
	 * @param null|string $expiry A strtotime() compatible cache time. If no expiry time is set,
	 *        then the default cache expiration time set with the cache configuration will be used.
	 * @return closure Function returning boolean `true` on successful write, `false` otherwise.
	 */
	public function write($key, $value = null, $expiry = null) {
		$connection =& $this->connection;
		$expiry = ($expiry) ?: $this->_config['expiry'];
		$_self =& $this;

		return function($self, $params) use (&$_self, &$connection, $expiry) {
			if (is_array($params['key'])) {
				$expiry = $params['data'];

				if ($connection->mset($params['key'])) {
					$ttl = array();

					if ($expiry) {
						foreach ($params['key'] as $k => $v) {
							$ttl[$k] = $_self->invokeMethod('_ttl', array($k, $expiry));
						}
					}
					return $ttl;
				}
			}
			if ($result = $connection->set($params['key'], $params['data'])) {
				if ($expiry) {
					return $_self->invokeMethod('_ttl', array($params['key'], $expiry));
				}
				return $result;
			}
		};
	}

	/**
	 * Read value(s) from the cache
	 *
	 * @param string $key The key to uniquely identify the cached item
	 * @return closure Function returning cached value if successful, `false` otherwise
	 */
	public function read($key) {
		$connection =& $this->connection;

		return function($self, $params) use (&$connection) {
			$key = $params['key'];

			if (is_array($key)) {
				return $connection->getMultiple($key);
			}
			return $connection->get($key);
		};
	}

	/**
	 * Delete value from the cache
	 *
	 * @param string $key The key to uniquely identify the cached item
	 * @return closure Function returning boolean `true` on successful delete, `false` otherwise
	 */
	public function delete($key) {
		$connection =& $this->connection;

		return function($self, $params) use (&$connection) {
			return (boolean) $connection->delete($params['key']);
		};
	}

	/**
	 * Performs an atomic decrement operation on specified numeric cache item.
	 *
	 * Note that if the value of the specified key is *not* an integer, the decrement
	 * operation will have no effect whatsoever. Redis chooses to not typecast values
	 * to integers when performing an atomic decrement operation.
	 *
	 * @param string $key Key of numeric cache item to decrement
	 * @param integer $offset Offset to decrement - defaults to 1.
	 * @return closure Function returning item's new value on successful decrement, else `false`
	 */
	public function decrement($key, $offset = 1) {
		$connection =& $this->connection;

		return function($self, $params) use (&$connection, $offset) {
			return $connection->decr($params['key'], $offset);
		};
	}

	/**
	 * Performs an atomic increment operation on specified numeric cache item.
	 *
	 * Note that if the value of the specified key is *not* an integer, the increment
	 * operation will have no effect whatsoever. Redis chooses to not typecast values
	 * to integers when performing an atomic increment operation.
	 *
	 * @param string $key Key of numeric cache item to increment
	 * @param integer $offset Offset to increment - defaults to 1.
	 * @return closure Function returning item's new value on successful increment, else `false`
	 */
	public function increment($key, $offset = 1) {
		$connection =& $this->connection;

		return function($self, $params) use (&$connection, $offset) {
			return $connection->incr($params['key'], $offset);
		};
	}

	/**
	 * Clears user-space cache
	 *
	 * @return mixed True on successful clear, false otherwise
	 */
	public function clear() {
		return $this->connection->flushdb();
	}

	/**
	 * Determines if the Redis extension has been installed and
	 * that there is a redis-server available
	 *
	 * @return boolean Returns `true` if the Redis extension is enabled, `false` otherwise.
	 */
	public static function enabled() {
		return extension_loaded('redis');
	}
}

?>
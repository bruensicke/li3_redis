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
	public static $_config = array();

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
	 * This method is called by `Redis::__init()`.
	 *
	 * @param array $options Possible options are:
	 *     - `format`: allows setting a prefix for keys, i.e. Environment
	 *
	 * @return void
	 */
	public static function config(array $options = array()) {
		$config = Libraries::get('li3_redis');
		$defaults = array(
			'expiry' => false,
			'connection' => 'li3_redis',
			'format' => '{:environment}:',
		);
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

	public static function connection($connection = null) {
		if (!is_null($connection)) {
			static::$connection = $connection;
		}
		return static::$connection;
	}

	public static function resolveFormat($name = null, $format = null) {
		$format = ($format) ? : static::$_config['format'];
		return String::insert($format, array(
			'name' => ($name) ? : '',
			'environment' => Environment::get(),
		));
	}

	/**
	 * returns a replaced version of a generic message format
	 *
	 * used to interpolate names/folders for keys
	 *
	 * @param string $name optional, if given, inserts the key
	 * @return string the parsed string
	 */
	public static function addKey($name = null, $namespace = null) {
		$name = ($namespace) ? sprintf('%s:%s', $namespace, $name) : $name;
		if (is_array($name)) {
			foreach($name as $key) {
				//TODO:
			}
		}
		return static::resolveFormat($name);
	}

	public static function removeKey($data, $namespace = null) {
		$name = static::resolveFormat($namespace);
		if (is_array($data)) {
			foreach ($data as $key => $value) {
				$newKey = trim(str_replace($name, '', $key), ':');
				$data[$newKey] = $value;
				unset($data[$key]);
			}
		}
		return $data;
	}

	/**
	 * searches for keys to look for
	 *
	 * @see RedisPlus::keys()
	 * @param string $search key search string
	 * @param string $namespace namespace in which to look for
	 * @return array an array containing all keys, that match the search string and their values
	 * @filter
	 */
	public static function find($search = '*', $namespace = null) {
		$params = compact('search', 'namespace', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			$search = $self::addKey($params['search'], $params['namespace']);
			$result = call_user_func_array(array($self::connection(), 'keys'), array($search));
			if (!$options['strip']) {
				return $result;
			}
			$name = $self::resolveFormat($params['namespace']);
			$return = array();
			foreach ($result as $value) {
				$return[] = trim(str_replace($name, '', $value), ':');
			}
			return $return;
		});
	}

	/**
	 * fetches existing keys and their corresponding values
	 *
	 * @see RedisPlus::keys()
	 * @param string $search key search string
	 * @param string $namespace namespace in which to look for
	 * @return array an array containing all keys, that match the search string and their values
	 * @filter
	 */
	public static function fetch($search = '*', $namespace = null) {
		$params = compact('search', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			$search = $self::addKey($params['search'], $params['namespace']);
			$keys = call_user_func_array(array($self::connection(), 'keys'), array($search));
			if (!is_array($keys)) {
				return array();
			}
			$values = call_user_func_array(array($self::connection(), 'getMultiple'), array($keys));
			if (!is_array($values)) {
				return array();
			}
			$result = array_combine($keys, $values);
			ksort($result);
			return $self::removeKey($result, $params['namespace']);
		});
	}

















	/**
	 * Sets expiration time for cache keys
	 *
	 * @param string $key The key to uniquely identify the cached item
	 * @param mixed $expiry A `strtotime()`-compatible string indicating when the cached item
	 *              should expire, or a Unix timestamp.
	 * @return boolean Returns `true` if expiry could be set for the given key, `false` otherwise.
	 */
	protected static function _ttl($key, $expiry) {
		return static::connection()->expireAt($key, is_int($expiry) ? $expiry : strtotime($expiry));
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
	public static function write($key, $value = null, $expiry = null) {
		$connection = static::connection();
		$expiry = ($expiry) ?: static::$_config['expiry'];
		$params = compact('key', 'value', 'expiry');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key']);
			$expiry = $params['expiry'];
			if (is_array($key)) {
				if ($connection->mset($key)) {
					$ttl = array();

					if ($expiry) {
						foreach ($key as $k => $v) {
							$ttl[$k] = $self::invokeMethod('_ttl', array($k, $expiry));
						}
					}
					return $ttl;
				}
			}
			if ($result = $connection->set($key, $params['value'])) {
				if ($expiry) {
					return $self::invokeMethod('_ttl', array($key, $expiry));
				}
				return $result;
			}
		});
	}

	/**
	 * Read value(s) from the cache
	 *
	 * @param string $key The key to uniquely identify the cached item
	 * @return closure Function returning cached value if successful, `false` otherwise
	 */
	public static function read($key, $namespace = null) {
		$connection = static::connection();
		$params = compact('key', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
			if (is_array($params['key'])) {
				return $connection->getMultiple($key);
			}
			return $connection->get($key);
		});
	}

	/**
	 * Delete value from the cache
	 *
	 * @param string $key The key to uniquely identify the cached item
	 * @return closure Function returning boolean `true` on successful delete, `false` otherwise
	 */
	public static function delete($key) {
		$connection =& static::connection();

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
	public static function decrement($key, $offset = 1) {
		$connection =& static::connection();

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
	public static function increment($key, $offset = 1) {
		$connection =& static::connection();

		return function($self, $params) use (&$connection, $offset) {
			return $connection->incr($params['key'], $offset);
		};
	}

	/**
	 * Clears user-space cache
	 *
	 * @return mixed True on successful clear, false otherwise
	 */
	public static function clear() {
		return static::connection()->flushdb();
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
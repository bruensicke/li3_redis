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
	 * Configures the redis backend for use as well as some application specific things.
	 *
	 * Most notably that will the format, which is a global namespace for the application itself.
	 *
	 * This method is called by `Redis::__init()`.
	 *
	 * @param array $options Possible options are:
	 *     - `format`: allows setting a prefix for keys, i.e. Environment
	 *
	 * @return array
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

	/**
	 * returns the connection object to redis, or stores it as static property
	 *
	 * @param object $connection (optional) if passed, will be stored as static class property
	 * @return object the current redis connection object
	 */
	public static function connection($connection = null) {
		if (!is_null($connection)) {
			return static::$connection = $connection;
		}
		return static::$connection;
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
			foreach ($name as $key => $current) {
				$data[$key] = static::resolveFormat($current);
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

	public static function resolveFormat($name = null, $format = null) {
		$format = ($format) ? : static::$_config['format'];
		return trim(String::insert($format, array(
			'name' => ($name) ? : '',
			'environment' => Environment::get(),
		)), ':');
	}

	/**
	 * searches for keys to look for
	 *
	 * if you pass in a namespace, all keys that are returned are manipulated so, that the namespace
	 * itself does not occur in the keys. That way, you can easily have sub-namespaces in your
	 * application. If you want to update values for these fields, just pass in the same namespace
	 * and everything will work as expected.
	 *
	 * @param string $search key search string
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @param array $options Possible options are:
	 *     - `raw`: supresses stripping namespace and format out of of keys, defaults to false
	 * @return array an array containing all keys, that match the search string and their values
	 * @filter
	 */
	public static function find($search = '*', $namespace = null, array $options = array()) {
		$connection = static::connection();
		$params = compact('search', 'namespace', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$defaults = array('raw' => false);
			$params['options'] += $defaults;
			$search = $self::addKey($params['search'], $params['namespace']);
			$result = $connection->keys($search);
			if ($params['options']['raw']) {
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
	 * if you pass in a namespace, all keys that are returned are manipulated so, that the namespace
	 * itself does not occur in the keys. That way, you can easily have sub-namespaces in your
	 * application. If you want to update values for these fields, just pass in the same namespace
	 * and everything will work as expected.
	 *
	 * @param string $search key search string
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @return array an array containing all keys, that match the search string and their values
	 * @filter
	 */
	public static function fetch($search = '*', $namespace = null) {
		$connection = static::connection();
		$params = compact('search', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$search = $self::addKey($params['search'], $params['namespace']);
			$keys = $connection->keys($search);
			if (!is_array($keys)) {
				return array();
			}
			$values = $connection->getMultiple($keys);
			if (!is_array($values)) {
				return array();
			}
			$result = array_combine($keys, $values);
			ksort($result);
			foreach($result as $key => $value) {
				if ($value !== false) {
					continue;
				}
				if ($connection->hLen($key)) {
					$result[$key] = $connection->hGetAll($key);
				}
			}
			return $self::removeKey($result, $params['namespace']);
		});
	}

	/**
	 * fetches fields from hashes, that are defined by a key search pattern
	 *
	 * if you pass in a namespace, all keys that are returned are manipulated so, that the namespace
	 * itself does not occur in the keys. That way, you can easily have sub-namespaces in your
	 * application. If you want to update values for these fields, just pass in the same namespace
	 * and everything will work as expected.
	 *
	 * @param string $search key search string
	 * @param string $field field to retrieve
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @return array an array containing all keys, matching the search string and their field values
	 * @filter
	 */
	public static function fetchHashFields($search = '*', $field, $namespace = null) {
		$connection = static::connection();
		$params = compact('search', 'field', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$keys = $self::find($params['search'], $params['namespace'], array('raw' => true));
			$result = array();
			if (!is_array($keys)) {
				return $result;
			}
			foreach($keys as $key) {
				$result[$key] = $connection->hGet($key, $params['field']);
			}
			return $self::removeKey($result, $params['namespace']);
		});
	}


	/**
	 * Write value(s) to the redis
	 *
	 * if value is of type array, writeHash will be automatically called. If you give a namespace
	 * the returning keys will be stripped of with that namespace.
	 *
	 * @see li3_redis\storage\Redis::writeHash()
	 * @param string $key The key to uniquely identify the redis item
	 * @param mixed $value The value to be stored in redis
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @param null|string $expiry A strtotime() compatible redis time. If no expiry time is set,
	 *        then the default redis expiration time set with the redis configuration will be used.
	 * @return closure Function returning boolean `true` on successful write, `false` otherwise.
	 */
	public static function write($key, $value = null, $namespace = null, $expiry = null) {
		$connection = static::connection();
		$expiry = ($expiry) ?: static::$_config['expiry'];
		$params = compact('key', 'value', 'namespace', 'expiry');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
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
			if (is_array($params['value'])) {
				return $self::invokeMethod('writeHash', array_values($params));
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
	 * increments all numeric values in a Hashmap in redis
	 *
	 * If you give an array of values to be incremented on given hash (identified by key) all
	 * increments will take the type into account, meaning a float value will be added via
	 * hIncrByFloat whereas an integer will be incremented as hIncrBy. Note, that redis v2.6 is
	 * needed for that to work properly. The check is not done via is_float or is_int (as this
	 * would force you to typecast all values in advance) but via a check against a dot in the
	 * numeric value instead. The check is done on the given value, not the one probably already
	 * present in redis.
	 *
	 * @param string $key The key to uniquely identify the redis hash
	 * @param array $value The values to be incremented in redis hashmap
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @param null|string $expiry A strtotime() compatible redis time. If no expiry time is set,
	 *        then the default redis expiration time set with the redis configuration will be used.
	 * @return array the updated values of all Hash fields stored in redis for given key
	 * @filter
	 */
	public static function incrementHash($key, $value = array(), $namespace = null, $expiry = null) {
		$connection = static::connection();
		$expiry = ($expiry) ?: static::$_config['expiry'];
		$params = compact('key', 'value', 'namespace', 'expiry');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
			$expiry = $params['expiry'];
			$result = array();
			if (!is_array($params['value'])) {
				// TODO: if !is_array()!
			}
			foreach ($params['value'] as $field => $val) {
				switch (true) {
					case is_numeric($val):
						$method = (stristr($val, '.') === false) ? 'hIncrBy' : 'hIncrByFloat';
						$result[$field] = $connection->$method($key, $field, $val);
						break;
					// case is_string($val):
					default:
						$connection->hSet($key, $field, $val);
						$result[$field] = $connection->hGet($key, $field);
						break;

				}
			}
			if ($expiry) {
				return $self::invokeMethod('_ttl', array($key, $expiry));
			}
			return $result;
		});
	}

	/**
	 * writes an array as Hash into redis
	 *
	 * @param string $key The key to uniquely identify the redis hash
	 * @param array $value The values to be stored in redis as Hashmap
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @param null|string $expiry A strtotime() compatible redis time. If no expiry time is set,
	 *        then the default redis expiration time set with the redis configuration will be used.
	 * @return array the updated values of all Hash fields stored in redis for given key
	 * @filter
	 */
	public static function writeHash($key, array $value = array(), $namespace = null, $expiry = null) {
		$connection = static::connection();
		$expiry = ($expiry) ?: static::$_config['expiry'];
		$params = compact('key', 'value', 'namespace', 'expiry');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
			$expiry = $params['expiry'];
			if ($success = $connection->hMset($key, $params['value'])) {
				if ($expiry) {
					$self::invokeMethod('_ttl', array($key, $expiry));
				}
				return $self::readHash($params['key'], $params['namespace']);
			}
		});
	}

	/**
	 * Read value(s) from redis
	 *
	 * If $key is an array, all keys and their values will be retrieved. In that case all keys must
	 * be of type string, because a getMultiple is issued against redis. If a value for a certain
	 * key is not of type string, unexpected behavior will occur.
	 *
	 * If $key points to a key that is of type Hashmap, Redis::readHash will be automatically called
	 * and the key and namespace will be handed over.
	 *
	 * @param string $key The key to uniquely identify the cached item
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @return array the updated values of all keys stored in redis for given key(s)
	 * @filter
	 */
	public static function read($key, $namespace = null) {
		$connection = static::connection();
		$params = compact('key', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
			if (is_array($params['key'])) {
				$values = $connection->getMultiple($key);
				$result = array_combine($params['key'], $values);
				ksort($result);
				foreach($result as $key => $value) {
					if ($value !== false) {
						continue;
					}
					if ($connection->hLen($key)) {
						$result[$key] = $connection->hGetAll($key);
					}
				}
				return $self::removeKey($result, $params['namespace']);
			}
			if ($connection->hLen($key)) {
				return $self::invokeMethod('readHash', array_values($params));
			}
			return $connection->get($key);
		});
	}

	/**
	 * reads an array from a Hashmap from redis
	 *
	 * This method will retrieve all fields and their values for a hashmap that is stored on a given
	 * key in redis.
	 *
	 * @param string $key The key to uniquely identify the redis hash
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @return array the values of all Hash fields stored in redis for given key
	 * @filter
	 */
	public static function readHash($key, $namespace = null) {
		$connection = static::connection();
		$params = compact('key', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
			return $connection->hGetAll($key);
		});
	}

	/**
	 * Delete value(s) from redis
	 *
	 * $key can be a string to identify one key or an array of keys to be deleted.
	 *
	 * @param string $key The key to uniquely identify the item in redis
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @return integer the number of values deleted from redis
	 * @filter
	 */
	public static function delete($key, $namespace = null) {
		$connection = static::connection();
		$params = compact('key', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
			return $connection->delete($key);
		});
	}

	/**
	 * Performs an atomic decrement operation on specified numeric redis item.
	 *
	 * Note that if the value of the specified key is *not* an integer, the decrement
	 * operation will have no effect whatsoever. Redis chooses to not typecast values
	 * to integers when performing an atomic decrement operation.
	 *
	 * @param string $key Key of numeric redis item to decrement
	 * @param integer $offset Offset to decrement - defaults to 1.
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @return integer|array new value of decremented item or an array of all values from redis
	 * @filter
	 */
	public static function decrement($key, $offset = 1, $namespace = null) {
		$connection = static::connection();
		$params = compact('key', 'offset', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
			return $connection->decr($key, $params['offset']);
		});
	}

	/**
	 * Performs an atomic increment operation on specified numeric redis item.
	 *
	 * Note that if the value of the specified key is *not* an integer, the increment
	 * operation will have no effect whatsoever. Redis chooses to not typecast values
	 * to integers when performing an atomic increment operation.
	 *
	 * @param string $key Key of numeric redis item to increment
	 * @param integer $offset Offset to increment - defaults to 1.
	 * @param string $namespace The application specific namespace to be prepend on the key
	 * @return integer|array new value of incremented item or an array of all values from redis
	 * @filter
	 */
	public static function increment($key, $offset = 1, $namespace = null) {
		$connection = static::connection();
		$params = compact('key', 'offset', 'namespace');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::addKey($params['key'], $params['namespace']);
			return $connection->incr($key, $params['offset']);
		});
	}

	/**
	 * Sets expiration time for redis keys
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
	 * Clears user-space redis
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
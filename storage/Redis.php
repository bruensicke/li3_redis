<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\storage;

use lithium\core\Libraries;
use lithium\core\Environment;
use lithium\data\Connections;
use lithium\util\String;
use lithium\util\Set;

/**
 * Redis class allows easy storing and retrieval of data with Redis
 *
 * It has a built-in functionality to allow for namespacing, prefixing and bucketing keys.
 * Some of these functionality may seem redundant, but you do not have to combine them all, you
 * can just use the one, that works best for you. If you are a heavy Redis-user, though, you will
 * come to the point, where a combination of all of these makes totally sense.
 *
 * But before we get into the details, lets dive into what this class does for you in general:
 *
 * - read/write/delete keys
 * - read/write/delete hashes
 * - read/write/delete fields from/into hashes
 * - increment/decrement keys
 * - increment/decrement fields within hashes
 *
 * If you worked with Hashes and incrementing values in there, you will probably have a good time
 * with Stats, which is a class that allows to store, increment, decrement values for stats of all
 * kinds.
 *
 * To understand how a key within redis is built, we expect you to understand some things:
 *
 * - all things in redis have one and only one key. There is no such thing as nesting or trees.
 * - all things in redis can be read, written and deleted via this unique key
 * - all things in redis can be set to removed automatically after a certain time-period, depending
 *   on the key.
 *
 * You probably should have grasped, that making up the correct key is important in redis. You are
 * absolutely right, but we are here to assist you. We want to make your life easier. And to achieve
 * that we built in a lot of key-generating functionality, to ease the pain of scoping, separating
 * and bucketing data. But let's get started easy.
 *
 * We have different words for different ways of prefixing, namespacing and manipulating keys:
 *
 * - format
 * - namespace
 * - prefix, aka bucket
 *
 * We are sure, that if you store data in redis of various kinds, you will make sure, they don't
 * interfere with each other. So you separate the keys, with words, to understand what belongs where
 * and that is totally, what you should do. By convention the colon `:` is used to stick together
 * areas or types of interest, like this: `application:config:name` - e.g. that key could store a certain
 * value that represents the name of the given application.
 *
 * We assume, you may want to run different applications and/or stages within the same Redis.
 * It is easy to use the built-in database functionality for that (and you can still use this) but
 * it is not necessary if you use li3_redis.
 *
 * So, in order to separate several applications/stages from each other you can define a format
 * that defines the overall key-structure. That format scopes the whole application into a container
 * with a certain prefix, that defaults to `environment`. It can be defined via the Libraries::add:
 *
 *		`Libraries::add('li3_redis', array('format' => 'app:{:environment}:{:name}'));`
 *
 * So, everything that we store in Redis, will be prefixed with that. So all keys, starting with
 * that will belong together. If we return keys to you, we will always strip the format out, so you
 * do not have to deal with long, unhandy keys. But you have to be aware, that the key, that is
 * presented to you is not the `real-for-real` key within Redis.
 *
 * Next up is `namespace`: For a certain application you will probably save information of different
 * kind. You want to namespace all these information, according to type, to easier see what belongs
 * where. Let's say you want to store some client-relevant information, as well as some statistics.
 *
 * Your applications container (see #format) would look like this:
 *
 * - root
 * + clients
 * + whatever
 * + stats
 *
 * Every key, that will be stored regarding to client, will be prefixed with your applications
 * format as well as the word `client`. That makes separation of concerns easy.
 *
 * We encounter it typical, that some of the models find their way as a namespace into the redis
 * usually accomponied with `stats` (we have something for you, if you like stats) and probably
 * `leaderboards` if you are into that kind of business. Because you probably access certain parts
 * of your application with Components relating to that, we strip out the namespace of the keys.
 * If you really want to have the namespace beeing presented to you, have a look at `prefixes`.
 *
 * Now for the pro`s:
 *
 * You will probably want to count certain things, like how many api requests have been made or
 * how often you do a certain action within your application. Easy. Just do `Redis::inc()` and
 * you are all set. Now imagine, you want to track these data, not only in general, but also for
 * a certain client, user or a scope like year, day of week or any other dimension you may need
 * useful. That is, where prefixes, aka buckets come in. They save you a lot of time, by
 * automatically prepending a certain key-prefix, that specifies the scope it belongs to.
 *
 * Let's say, you want to track api-requests. Look at this:
 *
 * {{{
 *	Stats::value('requests', 'successful');
 *	Stats::value('requests', 'successful', array('user' => 'foo'));
 *	Stats::value('requests', 'successful', array('user' => 'foo', 'year' => date('Y')));
 * }}}
 *
 * You end up with three keys within redis, all ending with 'requests', all starting with whatever
 * the namespace and configured format is, but having a different section to scope data for the
 * three scopes, that we can see here: `global`, `user` and `year`. The keys could look like this:
 *
 * app:stats:global:requests
 * app:stats:user:foo:requests
 * app:stats:year:2013:requests
 *
 * That way, you can count independently how many requests your application handled, how many have
 * been issued by a certain user and how many have been handled in which year. Nifty, hu?
 *
 * Because the scopes represent a total different view on data, we do not strip it out of keys
 * presented to you, but leave it intact. So you can actually work with these buckets.
 *
 * Have fun using Redis.
 *
 */
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
	 * This method is called by `Redis::__init()`.
	 *
	 * @param array $options Possible options are:
	 *     - `expiry`: allows setting a default expiration
	 *     - `namespace`: makes overwriting or re-configuring the namespace easy
	 *     - `connection`: defines, what connection to use, defaults to li3_redis
	 *     - `separator`: in case you prefer a different separator, defaults to `:`
	 *     - `format`: allows setting a prefix for keys, e.g. Environment
	 * @return array
	 */
	public static function config(array $options = array()) {
		$config = Libraries::get('li3_redis');
		$defaults = array(
			'expiry' => false,
			'namespace' => '',
			'connection' => 'li3_redis',
			'separator' => ':',
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
	 * searches for keys with a given prefix
	 *
	 * @param string $search key search string
	 * @param array $options array with additional options, see Redis::getKey()
	 *     - `raw`: supresses stripping namespace and format out of of keys, defaults to false
	 * @return array an array containing all keys, that match the search string and their values
	 * @filter
	 */
	public static function find($search = '*', array $options = array()) {
		$connection = static::connection();
		$params = compact('search', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$defaults = array('raw' => false);
			$params['options'] += $defaults;
			$result = $connection->keys($self::getKey($params['search'], $params['options']));
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
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return array an array containing all keys, that match the search string and their values
	 * @filter
	 */
	public static function fetch($search = '*', array $options = array()) {
		$connection = static::connection();
		$params = compact('search', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$search = $self::addKey($params['search'], $params['options']);
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
			return $self::resolveFormat($result, $params['options']);
		});
	}

	/**
	 * fetches fields from hashes, that are defined by a key search pattern
	 *
	 * Imagine, you have a series of buckets containing data. You probably want to aggregate some
	 * fields that are stored in hashmaps for these. This method helps you.
	 *
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $search key search string
	 * @param string $field field to retrieve
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return array an array containing all keys, matching the search string and their field values
	 * @filter
	 */
	public static function fetchHashFields($search = '*', $field, array $options = array()) {
		$connection = static::connection();
		$params = compact('search', 'field', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$params['options']['raw'] = true;
			$keys = $self::find($params['search'], $params['options']);
			$result = array();
			if (!is_array($keys)) {
				return $result;
			}
			foreach($keys as $key) {
				$result[$key] = $connection->hGet($key, $params['field']);
			}
			return $self::cleanKeys($result, $params['options']);
		});
	}

	/**
	 * Write value(s) to the redis
	 *
	 * If value is of type array, writeHash will be automatically called. If key is of type array,
	 * we assume that the array contains keys, which will be created as redis-keys and their
	 * corresponding values will be written. Please make sure, that is a difference and you use
	 * the method in the intended way.
	 *
	 * If you are writing a lot of keys, you should consider creating a hash and store your values
	 * in there. Redis is much faster in managing large hashes, than in having a huge amount of keys
	 * but it totally depends on your application and use-cases, of course.
	 *
	 * some examples how to write data to redis:
	 *
	 * {{{
	 *    Redis::write('key', 'value'); // save `value` at key `key`
	 *    Redis::write(array('key' => 'value')); // same as above
	 *    Redis::write(array('key1' => 'value1', 'key2' => 'value2')); // write multiple in one call
	 *    Redis::write('key', array('field1' => 123)); // creates Hash, instead of string in redis
	 *    Redis::write('key', array('field1' => 'value1', 'field2' => 'value2'));
	 *    Redis::write('key', array('field1' => 123), array('expiry' => '+1 hour'));
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @see li3_redis\storage\Redis::writeHash()
	 * @param string|array $key The key to uniquely identify the redis item, if array is given every
	 *        key in array will be created, with their corresponding values as value.
	 * @param string|array $value The value to be stored in redis, can be either a string or an array.
	 *        if value is an array, a hash is created in redis at given key.
	 * @param array $options array with additional options, see Redis::getKey()
	 *     - `expiry`: A strtotime() compatible redis time. If no expiry time is set,
	 *        then the default redis expiration time set with the redis configuration will be used.
	 * @return array|string the updated value(s) of all fields stored in redis for given key(s)
	 * @filter
	 */
	public static function write($key, $value = null, array $options = array()) {
		$connection = static::connection();
		$defaults = array('expiry' => static::$_config['expiry']);
		$options += $defaults;
		$params = compact('key', 'value', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
			if (is_array($params['key'])) {
				if (!$connection->mset($key)) {
					return false;
				}
				if ($params['options']['expiry']) {
					$self::_ttl($key, $params['options']['expiry']);
				}
				return $self::read(array_keys($params['key']), $params['options']);
			}
			if (is_array($params['value'])) {
				return $self::invokeMethod('writeHash', array_values($params));
			}
			if ($result = $connection->set($key, $params['value'])) {
				if ($params['options']['expiry']) {
					$self::_ttl($key, $params['options']['expiry']);
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
	 * If you pass in values, that are not numeric, a normal set will be issued, so the value will
	 * be stored, but obviously not incremented, but overwritten.
	 *
	 * You can also just increment by one, on one field on a hash, identified by `$key` if you pass
	 * in a string, instead of an array as `$values`. See examples for usage:
	 *
	 * {{{
	 *    Redis::incrementHash($key, 'fieldname'); // will increment by 1
	 *    Redis::incrementHash($key, array('field1' => 123));
	 *    Redis::incrementHash($key, array('field1' => 123), array('expiry' => '+1 hour'));
	 *    Redis::incrementHash($key, array('field1' => 1, 'field2' => 'value2'));
	 * }}}
	 *
	 * Tip: If you pass in an additional field with a datetime string, e.g. date(DATE_ATOM) you will
	 * be able to determine, when the last update to a hash has been issued.
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the redis hash
	 * @param array $values The values to be incremented in redis hashmap
	 * @param array $options array with additional options, see Redis::getKey()
	 *     - `expiry`: A strtotime() compatible redis time. If no expiry time is set,
	 *        then the default redis expiration time set with the redis configuration will be used.
	 * @return array the updated values of all Hash fields stored in redis for given key
	 * @filter
	 */
	public static function incrementHash($key, $values = array(), array $options = array()) {
		$connection = static::connection();
		$defaults = array('expiry' => static::$_config['expiry']);
		$options += $defaults;
		$params = compact('key', 'values', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
			$result = array();

			if (!is_array($params['values'])) {
				return $connection->hIncrBy($key, $params['values'], 1);
			}

			foreach ($params['values'] as $field => $val) {
				switch (true) {
					case is_numeric($val):
						$method = (stristr($val, '.') === false) ? 'hIncrBy' : 'hIncrByFloat';
						$result[$field] = $connection->$method($key, $field, $val);
						break;
					default:
						$connection->hSet($key, $field, $val);
						$result[$field] = $connection->hGet($key, $field);
						break;
				}
			}
			if ($params['options']['expiry']) {
				$self::_ttl($key, $params['options']['expiry']);
			}
			return $result;
		});
	}

	/**
	 * decrements all numeric values in a Hashmap in redis
	 *
	 * If you give an array of values to be decremented on given hash (identified by key) all
	 * decrements will take the type into account, meaning a float value will be added via
	 * hIncrByFloat whereas an integer will be decremented as hIncrBy. Note, that redis v2.6 is
	 * needed for that to work properly. The check is not done via is_float or is_int (as this
	 * would force you to typecast all values in advance) but via a check against a dot in the
	 * numeric value instead. The check is done on the given value, not the one probably already
	 * present in redis.
	 *
	 * If you pass in values, that are not numeric, a normal set will be issued, so the value will
	 * be stored, but obviously not decremented, but overwritten.
	 *
	 * You can also just decrement by one, on one field on a hash, identified by `$key` if you pass
	 * in a string, instead of an array as `$values`. See examples for usage:
	 *
	 * {{{
	 *    Redis::decrementHash($key, 'fieldname'); // will decrement by 1
	 *    Redis::decrementHash($key, array('field1' => 123));
	 *    Redis::decrementHash($key, array('field1' => 123), array('expiry' => '+1 hour'));
	 *    Redis::decrementHash($key, array('field1' => 1, 'field2' => 'value2'));
	 * }}}
	 *
	 * Tip: If you pass in an additional field with a datetime string, e.g. date(DATE_ATOM) you will
	 * be able to determine, when the last update to a hash has been issued.
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the redis hash
	 * @param array $values The values to be decremented in redis hashmap
	 * @param array $options array with additional options, see Redis::getKey()
	 *     - `expiry`: A strtotime() compatible redis time. If no expiry time is set,
	 *        then the default redis expiration time set with the redis configuration will be used.
	 * @return array the updated values of all Hash fields stored in redis for given key
	 * @filter
	 */
	public static function decrementHash($key, $values = array(), array $options = array()) {
		$connection = static::connection();
		$defaults = array('expiry' => static::$_config['expiry']);
		$options += $defaults;
		$params = compact('key', 'values', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
			$result = array();

			if (!is_array($params['values'])) {
				return $connection->hIncrBy($key, $params['values'], -1);
			}

			foreach ($params['values'] as $field => $val) {
				switch (true) {
					case is_numeric($val):
						$method = (stristr($val, '.') === false) ? 'hIncrBy' : 'hIncrByFloat';
						$result[$field] = $connection->$method($key, $field, -$val);
						break;
					default:
						$connection->hSet($key, $field, $val);
						$result[$field] = $connection->hGet($key, $field);
						break;
				}
			}
			if ($params['options']['expiry']) {
				$self::_ttl($key, $params['options']['expiry']);
			}
			return $result;
		});
	}

	/**
	 * writes an array as Hash into redis
	 *
	 * Values is an associated array with fieldnames and their values. If you want to store more
	 * than one value at a time, just pass them all with your values array as second parameter.
	 *
	 * examples of usage:
	 *
	 * {{{
	 *    Redis::writeHash($key, array('field1' => 'value1'));
	 *    Redis::writeHash($key, array('field1' => 'value1'), array('expiry' => '+1 hour'));
	 *    Redis::writeHash($key, array('field1' => 'value1', 'field2' => 'value2'));
	 * }}}
	 *
	 * Tip: If you need to store nested data, you could serialize the value.
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the redis hash
	 * @param array $values The values to be stored in redis as Hashmap
	 * @param array $options array with additional options, see Redis::getKey()
	 *     - `expiry`: A strtotime() compatible redis time. If no expiry time is set,
	 *        then the default redis expiration time set with the redis configuration will be used.
	 * @return array the updated values of all Hash fields stored in redis for given key
	 * @filter
	 */
	public static function writeHash($key, array $values = array(), array $options = array()) {
		$connection = static::connection();
		$defaults = array('expiry' => static::$_config['expiry']);
		$options += $defaults;
		$params = compact('key', 'values', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
			if ($success = $connection->hMset($key, $params['values'])) {
				if ($params['options']['expiry']) {
					$self::_ttl($key, $params['options']['expiry']);
				}
				return $self::readHash($params['key'], '', $params['options']);
			}
		});
	}

	/**
	 * Read value(s) from redis
	 *
	 * If $key is an array, all keys and their values will be retrieved. In that case all keys must
	 * be of type string, because a getMultiple is issued against redis. If a value for a certain
	 * key is not of type string or Hashmap, unexpected behavior will occur.
	 *
	 * If $key points to a key that is of type Hashmap, Redis::readHash will be automatically called
	 * and all fields within that Hashmap returned.
	 *
	 * {{{
	 *    Redis::read($key, array('prefix' => 'foo'));
	 *    Redis::read($key, array('prefix' => 'foo', 'namespace' => 'bar'));
	 *    Redis::read(array('key1', 'key2'));
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string|array $key The key to uniquely identify the item in redis, or an array thereof
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return array the updated values of all keys stored in redis for given key(s)
	 * @filter
	 */
	public static function read($key, array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
			if (is_array($key)) {
				$values = $connection->getMultiple($key);
				$result = array_combine($params['key'], $values);
				ksort($result);
				foreach($result as $key => $value) {
					if ($value !== false) {
						continue;
					}
					$redisKey = $self::getKey($key, $params['options']);
					if ($connection->hLen($redisKey)) {
						$result[$key] = $connection->hGetAll($redisKey);
					}
				}
				return $result;
			}
			if ($connection->hLen($key)) {
				return $self::readHash($params['key'], '', $params['options']);
			}
			return $connection->get($key);
		});
	}

	/**
	 * reads an array from a Hashmap from redis
	 *
	 * This method will retrieve all fields and their values for a hashmap that is stored on a given
	 * key in redis. If you do not provide fields to fetch, all fields at given hash will be
	 * returned. You can just request one field as a string or an array with strings to retrieve
	 * more fields with one call.
	 *
	 * {{{
	 *    Redis::readHash($key, 'field1');
	 *    Redis::readHash($key, array('field1'));
	 *    Redis::readHash($key, array('field1', 'field2'));
	 * }}}
	 *
	 * Please take note, that the value is returned directly, if you just request one field.
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the Hashmap in redis
	 * @param string|array $fields if provided, only these fields from hash will be returned
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return array the values of all Hash fields stored in redis for given key
	 * @filter
	 */
	public static function readHash($key, $fields = '', array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'fields', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
			if (!empty($params['fields'])) {
				$fields = (!is_array($params['fields']))
					? array($params['fields'])
					: $params['fields'];
				$result = array();
				foreach((array) $fields as $field) {
					$result[$field] = $connection->hGet($key, $field);
				}
				return (count($result) === 1) ? $result[$field] : $result;
			}
			return $connection->hGetAll($key);
		});
	}

	/**
	 * get number of values within a hash from redis
	 *
	 * {{{
	 *    Redis::hashLength('key');
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer|boolean the number of values in that hash, false if key does not exist
	 * @filter
	 */
	public static function hashLength($key, array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			return $connection->hLen($self::getKey($params['key'], $params['options']));
		});
	}

	/**
	 * get sum of values within a hash from redis
	 *
	 * {{{
	 *    Redis::hashSum('key');
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer the sum of all numeric values for given hash
	 * @filter
	 */
	public static function hashSum($key, array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			return array_sum($connection->hVals($self::getKey($params['key'], $params['options'])));
		});
	}

	/**
	 * get all values from a hash from redis
	 *
	 * {{{
	 *    Redis::hashValues('key');
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return array the values of all fields for given hash
	 * @filter
	 */
	public static function hashValues($key, array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			return $connection->hVals($self::getKey($params['key'], $params['options']));
		});
	}

	/**
	 * Delete value(s) from redis
	 *
	 * $key can be a string to identify one key or an array of keys to be deleted. It does not
	 * matter what type of value is stored at given key.
	 *
	 * {{{
	 *    Redis::delete('key'); // remove value at key `key`
	 *    Redis::delete(array('key')); // same as above
	 *    Redis::delete(array('key1', 'key2')); // delete multiple keys at once
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer the number of values deleted from redis
	 * @filter
	 */
	public static function delete($key, array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			return $connection->del($self::getKey($params['key'], $params['options']));
		});
	}

	/**
	 * Delete value(s) from redis hashes
	 *
	 * $fields can be a string to identify one field or an array of fields to be deleted from hash.
	 * If you just pass in a string, only one field from the hash gets deleted.
	 *
	 * {{{
	 *    Redis::deleteFromHash($key, 'field1');
	 *    Redis::deleteFromHash($key, array('field1'));
	 *    Redis::deleteFromHash($key, array('field1', 'field2'));
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param string|array $fields The field to remove or an array of fields
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return boolean|array true in case of success for one field, false in case of error, e.g. key
	 *         not found or field at that hash not found. If more than one field is given, an array
	 *         keyed off all fields and their success as value
	 * @filter
	 */
	public static function deleteFromHash($key, $fields, array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'fields', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
			$result = array();
			foreach((array) $params['fields'] as $field) {
				$result[$field] = $connection->hDel($key, $field);
			}
			return (count($result) === 1) ? $result[$field] : $result;
		});
	}

	/**
	 * Performs an atomic decrement operation on specified numeric redis item.
	 *
	 * Note that if the value of the specified key is *not* an integer, the decrement
	 * operation will have no effect whatsoever. Redis chooses to not typecast values
	 * to integers when performing an atomic decrement operation.
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key Key of numeric redis item to decrement
	 * @param integer $offset Offset to decrement - defaults to 1.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer|array new value of decremented item or an array of all values from redis
	 * @filter
	 */
	public static function decrement($key, $offset = 1, array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'offset', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
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
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key Key of numeric redis item to increment
	 * @param integer $offset Offset to increment - defaults to 1.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer|array new value of incremented item or an array of all values from redis
	 * @filter
	 */
	public static function increment($key, $offset = 1, array $options = array()) {
		$connection = static::connection();
		$params = compact('key', 'offset', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) use ($connection) {
			$key = $self::getKey($params['key'], $params['options']);
			return $connection->incr($key, $params['offset']);
		});
	}

	/**
	 * Sets expiration time for redis keys
	 *
	 * please note, the key must be identified full-qualified, i.e. not assuming whether format,
	 * namespace nor prefix will be applied.
	 *
	 * @param string $key The key to uniquely identify the redis item
	 * @param mixed $expiry A `strtotime()`-compatible string indicating when the cached item
	 *              should expire, or a Unix timestamp.
	 * @return boolean Returns `true` if expiry could be set for the given key, `false` otherwise.
	 */
	protected static function _ttl($key, $expiry) {
		if (is_array($key)) {
			$result = array();
			foreach ($key as $k) {
				$result[] = static::_ttl($k, $expiry);
			}
			return $result;
		}
		return static::connection()->expireAt($key, is_int($expiry) ? $expiry : strtotime($expiry));
	}

	/**
	 * Clears application specific space within redis
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @return boolean True on successful clear, false otherwise
	 */
	public static function clear($namespace = null) {
		// TODO: implement me
		return false;
	}

	/**
	 * Flushes the whole database, therefore removing any (!) data
	 *
	 * @return boolean True on successful flush, false otherwise
	 */
	public static function flush() {
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


	/**
	 * generates redis key with additional prefix prepended
	 *
	 * allows easy scoping of keys within redis, e.g. prepending a certain key
	 * with user_id, year, or environment
	 *
	 * {{{
	 *    Redis::getKey('foo');
	 *    Redis::getKey('foo', array('prefix' => 'bar'));
	 *    Redis::getKey('foo', array('namespace' => 'bar'));
	 *    Redis::getKey('foo', array('prefix' => 'bar', 'namespace' => 'baz'));
	 *    Redis::getKey(array('foo', 'bar'));
	 *    Redis::getKey(array('foo', 'bar'), array('format' => 'app:{:environment}'));
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @see li3_redis\storage\Redis::resolveFormat()
	 * @param string|array $key default key or an array thereof
	 * @param array $options array with additional options
	 *     - `prefix`: allows setting a prefix for keys, defaults to 'global', that is used to allow
	 *       easy setting of buckets, e.g. $prefix = array('user:foo', 'year:2013'), see
	 *     - `namespace`: namespace used within Redis, see docs about namespaces
	 * @return string|array a concatenation with prefix, a colon `:` and key, in case you requested
	 *         multiple keys at once, you will get an indexed array with all generated keys.
	 * @filter
	 */
	public static function getKey($key, array $options = array()) {
		$params = compact('key', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			if (is_array($params['key'])) {
				$result = array();
				foreach ($params['key'] as $k => $v) {
					if (!is_numeric($k)) {
						$newK = $self::getKey($k, $params['options']);
						$result[$newK] = $v;
						unset($result[$k]);
					} else {
						$result[] = $self::getKey($v, $params['options']);
					}
				}
				return $result;
			}
			if (!empty($params['options']['prefix'])) {
				$params['key'] = $self::addPrefix($params['key'], $params['options']['prefix']);
			}
			if (!empty($params['options']['namespace'])) {
				$params['key'] = $self::addPrefix($params['key'], $params['options']['namespace']);
			}
			return $self::resolveFormat($params['key'], $params['options']);
		});


	}

	/**
	 * removes all occurences of namespace and format-relevant parts of the key
	 *
	 * We make use of the underlying keys easier, by removing overhead that is not useful to
	 * the user and avoid handling of long keys.
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @see li3_redis\storage\Redis::resolveFormat()
	 * @param string $key default key, where data has been retrieved
	 * @param string|array $data an array of data, to be cleaned
	 * @param array $options array with additional options
	 *     - `raw`: if you do not want your keys to be stripped down, pass in raw
	 * @return string a concatenation with prefix, a colon `:` and key
	 */
	public static function cleanKeys($key, $data, array $options = array()) {
		$defaults = array();
		$options += $defaults;
		if (isset($options['raw'])) {
			return $data;
		}
		if (is_array($data)) {
			foreach ($data as $oldKey => $value) {
				$newKey = trim(str_replace($key, '', $oldKey), ':');
				$data[$newKey] = $value;
				unset($data[$oldKey]);
			}
			return $data;
		}
		return trim(str_replace($key, '', $data), ':');
	}

	/**
	 * generates redis key with additional prefix prepended
	 *
	 * allows easy scoping of keys within redis, e.g. prepending a certain key
	 * with user_id, year, or environment
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key default key
	 * @param string $prefix prefix to be prepended to key
	 * @param array $options array with additional options
	 *     - `separator`: defaults to colon: `:` - can be changed to whatever you like
	 * @return string a concatenation with prefix, a separator (defaults to `:`) and key
	 */
	public static function addPrefix($key, $prefix = '', array $options = array()) {
		$defaults = array('separator' => static::$_config['separator']);
		$options += $defaults;
		$result = (!empty($prefix)) ? sprintf('%s%s%s', $prefix, $options['separator'], $key) : $key;
		return trim($result, $options['separator']);
	}

	/**
	 * generates the correct key for redis taking the format into account and applies all
	 * replacements, if there are any
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key specific redis key, optional
	 * @param array $options array with additional options
	 *     - `format`: what format to resolve, defaults to `static::$_config['format']`
	 *     - `separator`: what separator to use, defaults to `static::$_config['format']`
	 *     - `replacements`: array of replacement keys and values
	 * @return string generated key, with all fields set
	 */
	public static function resolveFormat($key = null, array $options = array()) {
		$defaults = array(
			'format' => static::$_config['format'],
			'separator' => static::$_config['separator'],
			'replacements' => array(),
		);
		$options += $defaults;
		$format = (!stristr($options['format'], '{:key}'))
			? static::addPrefix('{:key}', $options['format'], $options)
			: $options['format'];
		$replacements = array_merge($options['replacements'], array(
			'{:key}' => ($key) ? : '',
			'{:environment}' => Environment::get(),
		));
		$result = str_replace(array_keys($replacements), array_values($replacements), $format);
		return trim($result, $options['separator']);
	}
}

?>
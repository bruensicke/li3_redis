<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_redis\storage;

use li3_redis\storage\Redis;

use lithium\util\Collection;

/**
 * Lists allow easy creation and manipulation of redis lists
 *
 * Let's say, you want to track api-requests. Look at this:
 *
 * {{{
 *	Lists::add('inbox', 'foo'); // adds foo to inbox
 *	Lists::add('inbox', array('foo', 'bar')); // adds (another) foo and bar to inbox
 *	Lists::get('inbox'); // returns foo, foo, bar
 * }}}
 *
 * you can also make use of the fantastic bucket-feature you already know and love from Stats.
 * Therefore you can create bucketed lists and or pull data from more than one list at once.
 *
 * {{{
 *   Lists::get('inbox', array('user' => 'foo', 'news')); // returns items from both locations
 * }}}
 *
 * All return values from get will be automatically wrapped in a lithium Collection object for your
 * iteration and manipulation pleasures.
 *
 * Have fun using Lists.
 *
 */
class Lists extends \lithium\core\StaticObject {

	/**
	 * what namespace to use in redis
	 *
	 * @var string
	 */
	public static $namespace = 'lists';

	/**
	 * add value(s) to a list in redis
	 *
	 * examples to add items include this:
	 *
	 * {{{
	 *   Lists::add('foo', 'bar'); // adds `bar` to a list called `foo`
	 *   Lists::add('foo', array('bar', 'baz')); // adds `bar` and `baz` to a list called `foo`
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::listAdd()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param string|array $values an item or an array of items to add to the list
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the number of items in the list, or an array on multiple buckets
	 * @filter
	 */
	public static function add($key, $values, $buckets = null, array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'values', 'buckets', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			if (is_null($buckets)) {
				return Redis::listAdd($key, $values, $options);
			}
			$result = array();

			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix)) ? Redis::addPrefix($val, $prefix) : $val;
				$result[$options['prefix']] = Redis::listAdd($key, $values, $options);
			}
			return (count($result) > 1) ? $result : array_shift($result);
		});
	}

	/**
	 * fetches items from lists
	 *
	 * {{{
	 *    Lists::get('inbox'); // return all elements of inbox
	 *    Lists::get('inbox', 4); // return 4th element of inbox
	 *    Lists::get('inbox', array(2, 5)); // return 2-5th element of inbox
	 *    Lists::get('inbox', array(10)); // return all remaining items, starting from 10
	 *    Lists::get('inbox', array(10, -1)); // same as above
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::listGet()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param integer|array $index index to fetch or a range
	 * @param string|array $buckets string or array with additional buckets, see Lists::add()
	 * @param array $options array with additional options, see Redis::getKey()
	 *     - `start`: on empty index, this start will be used
	 *     - `end`: on empty index, this end will be used
	 *     - `merge`: set to false, if you want your results to be returned on a per-bucket base.
	 *     - `collect`: set to false, to return an array, instead of an collection object
	 * @return object|array a lithium collection object, containing all values, or array of values
	 * @filter
	 */
	public static function get($key, $index = array(), $buckets = null, array $options = array()) {
		$defaults = array('collect' => true, 'merge' => true, 'namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'index', 'buckets', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			if (is_null($buckets)) {
				$data = Redis::listGet($key, $index, $options);
				return ($options['collect']) ? new Collection(compact('data')) : $data;
			}
			$data = array();
			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix)) ? Redis::addPrefix($val, $prefix) : $val;
				$row = Redis::listGet($key, $index, $options);
				if (!$options['merge']) {
					$data[$options['prefix']] = $row;
				} else {
					$data = array_merge($data, $row);
				}
			}
			return ($options['collect']) ? new Collection(compact('data')) : $data;
		});
	}

	/**
	 * set a value at a given index on a certain key
	 *
	 * {{{
	 *   Lists::set('foo', 3, 'bar'); // sets third value of `foo` to `bar`
	 *   Lists::set('foo', array(3 => 'bar')); // same as above
	 *   Lists::set('foo', array(3 => 'bar', 2 => 'baz')); // set multiple values at once
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::listSet()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param integer|array $index index to replace or an array with index and their value
	 * @param mixed $value value to be set on given index
	 * @param array $options array with additional options, see Redis::getKey()
	 * @filter
	 */
	public static function set($key, $index, $value = null, array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'index', 'value', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			return Redis::listSet($params['key'], $params['index'], $params['value'], $params['options']);
		});
	}

	/**
	 * Returns and removes the first|last element of the list.
	 *
	 * {{{
	 *   Lists::pop('incoming'); // gets first element from list `incoming`
	 *   Lists::pop('incoming', true); // gets first element from list `incoming` or waits until
	 *                                    it is present in a blocking manner
	 *   Lists::pop('incoming', false, array('last' => true)); // gets last element from list
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::listPop()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param boolean $blocking set to true, if you want to wait until an item appears in list
	 * @param array $options array with additional options, see Redis::getKey()
	 *     - `last`: set to true, if last element should be returned, defaults to false
	 * @return bool|string the first|last element from the list, false if non-existent
	 * @filter
	 */
	public static function pop($key, $blocking = false, array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'blocking', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			return Redis::listPop($params['key'], $params['blocking'], $params['options']);
		});
	}

	/**
	 * get amount of items within a list from redis
	 *
	 * {{{
	 *   Lists::count('key');
	 *   Lists::count('key', array('user' => 'foo', 'global')); // for multiple buckets at once
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::listLength()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer|boolean the number of values in that list, false if key does not exist
	 * @filter
	 */
	public static function count($key, $buckets = null, array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'buckets', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			if (is_null($buckets)) {
				return Redis::listLength($key, $options);
			}
			$result = array();
			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix)) ? Redis::addPrefix($val, $prefix) : $val;
				$result[$options['prefix']] = Redis::listLength($key, $options);
			}
			return (count($result) > 1) ? $result : array_shift($result);
		});
	}

	/**
	 * Returns and removes the last element of the list.
	 *
	 * @see li3_redis\storage\Lists::pop()
	 * @see li3_redis\storage\Redis::listPop()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param boolean $blocking set to true, if you want to wait until an item appears in list
	 * @param array $options array with additional options, see Redis::getKey()
	 *     - `last`: set to true, if last element should be returned, defaults to false
	 * @return bool|string the first|last element from the list, false if non-existent
	 * @filter
	 */
	public static function popLast($key, $blocking = false, array $options = array()) {
		$options['last'] = true;
		return static::pop($key, $blocking, $options);
	}

	/**
	 * add value(s) to the right (bottom) of a list in redis
	 *
	 * @see li3_redis\storage\Lists::add()
	 * @see li3_redis\storage\Redis::listAdd()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param string|array $values an item or an array of items to add to the list
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the number of items in the list, or an array on multiple buckets
	 * @filter
	 */
	public static function append($key, $data, $buckets = null, array $options = array()) {
		$options['prepend'] = false;
		return static::add($key, $data, $buckets, $options);
	}

	/**
	 * add value(s) to the right (bottom) of a list in redis, if list already exists
	 *
	 * @see li3_redis\storage\Lists::add()
	 * @see li3_redis\storage\Redis::listAdd()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param string|array $values an item or an array of items to add to the list
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the number of items in the list, or an array on multiple buckets
	 * @filter
	 */
	public static function appendIfExists($key, $data, $buckets = null, array $options = array()) {
		$options['prepend'] = false;
		$options['check'] = true;
		return static::add($key, $data, $buckets, $options);
	}

	/**
	 * add value(s) to the left (top) of a list in redis
	 *
	 * @see li3_redis\storage\Lists::add()
	 * @see li3_redis\storage\Redis::listAdd()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param string|array $values an item or an array of items to add to the list
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the number of items in the list, or an array on multiple buckets
	 * @filter
	 */
	public static function prepend($key, $data, $buckets = null, array $options = array()) {
		$options['prepend'] = true;
		return static::add($key, $data, $buckets, $options);
	}

	/**
	 * add value(s) to the left (top) of a list in redis, if list already exists
	 *
	 * @see li3_redis\storage\Lists::add()
	 * @see li3_redis\storage\Redis::listAdd()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param string|array $values an item or an array of items to add to the list
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the number of items in the list, or an array on multiple buckets
	 * @filter
	 */
	public static function prependIfExists($key, $data, $buckets = null, array $options = array()) {
		$options['prepend'] = true;
		$options['check'] = true;
		return static::add($key, $data, $buckets, $options);
	}

	/**
	 * add value(s) to a list in redis, if list already exists
	 *
	 * @see li3_redis\storage\Lists::add()
	 * @see li3_redis\storage\Redis::listAdd()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param string|array $values an item or an array of items to add to the list
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the number of items in the list, or an array on multiple buckets
	 * @filter
	 */
	public static function addIfExists($key, $data, $buckets = null, array $options = array()) {
		$options['check'] = true;
		return static::add($key, $data, $buckets, $options);
	}

	/**
	 * get amount of items within a list from redis
	 *
	 * @see li3_redis\storage\Lists::count()
	 * @see li3_redis\storage\Redis::listLength()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer|boolean the number of values in that list, false if key does not exist
	 * @filter
	 */
	public static function size($key, $buckets = null, array $options = array()) {
		return static::count($key, $buckets, $options);
	}

	/**
	 * get amount of items within a list from redis
	 *
	 * @see li3_redis\storage\Lists::count()
	 * @see li3_redis\storage\Redis::listLength()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key The key to uniquely identify the item in redis
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer|boolean the number of values in that list, false if key does not exist
	 * @filter
	 */
	public static function length($key, $buckets = null, array $options = array()) {
		return static::count($key, $buckets, $options);
	}



}
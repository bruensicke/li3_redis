<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_redis\storage;

use li3_redis\storage\Redis;
use lithium\core\Libraries;

/**
 * Stats allow easy counting of metrics and application-specific events.
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
 * Have fun using Stats.
 *
 */
class Stats extends \lithium\core\StaticObject {

	/**
	 * what namespace to use in redis
	 *
	 * @var string
	 */
	public static $namespace = 'stats';

	/**
	 * increases the amount of given value(s)
	 *
	 * The usage is for bucketed stats, e.g. you want to count how many api-requests are made,
	 * you can count that gor a global bucket (default) and with the same call you can count
	 * api-calls for a specific range, e.g. user, year or resource.
	 *
	 * examples:
	 *
	 * {{{
	 *	Stats::inc('api', 'calls');
	 *	Stats::inc('api', array('calls' => 1, 'successful' => 1));
	 *	Stats::inc('api', array('calls' => 1, 'successful' => 1), array('user' => 'foo'));
	 *	Stats::inc('api', array('calls' => 1), array('user' => 'foo', 'year' => date('Y')));
	 *	Stats::inc('api', array('calls' => 1), 'prefix');
	 *	Stats::inc('api', array('calls' => 1), array('prefix'));
	 *	Stats::inc('api', array('calls' => 1), array('prefix1', 'prefix2'));
	 *	Stats::inc('api', array('calls' => 1), array('prefix1'), array('namespace' => 'foo'));
	 * }}}
	 *
	 * Please note, if you pass in buckets, the global bucket will always be incremented as well.
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param string|array $values an array of values to increase, the format is
	 *        array($name => $value) and can contain an unlimited number of stats to increase.
	 *        Can also be a plain string - in that case, we assume you want to increment by 1.
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the value that has been incremented or an array of values
	 * @filter
	 */
	public static function inc($key, $values, $buckets = 'global', array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'values', 'buckets', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			$result = array();

			$values = (is_string($values)) ? array($values => 1): $values;
			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix)) ? Redis::addPrefix($val, $prefix) : $val;
				$result[$options['prefix']] = Redis::incrementHash($key, $values, $options);
			}
			return (count($result) > 1) ? $result : array_shift($result);
		});
	}

	/**
	 * decreases the amount of given value(s)
	 *
	 * The usage is for bucketed stats, e.g. you want to count how many api-requests are made,
	 * you can count that gor a global bucket (default) and with the same call you can count
	 * api-calls for a specific range, e.g. user, year or resource.
	 *
	 * examples:
	 *
	 * {{{
	 *	Stats::dec('api', 'calls');
	 *	Stats::dec('api', array('calls' => 1, 'successful' => 1));
	 *	Stats::dec('api', array('calls' => 1, 'successful' => 1), array('user' => 'foo'));
	 *	Stats::dec('api', array('calls' => 1), array('user' => 'foo', 'year' => date('Y')));
	 *	Stats::dec('api', array('calls' => 1), 'prefix');
	 *	Stats::dec('api', array('calls' => 1), array('prefix'));
	 *	Stats::dec('api', array('calls' => 1), array('prefix1', 'prefix2'));
	 *	Stats::dec('api', array('calls' => 1), array('prefix1'), array('namespace' => 'foo'));
	 * }}}
	 *
	 * Please note, if you pass in buckets, the global bucket will always be incremented as well.
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param string|array $values an array of values to decrease, the format is
	 *        array($name => $value) and can contain an unlimited number of stats to decrease.
	 *        Can also be a plain string - in that case, we assume you want to increment by 1.
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the value that has been incremented or an array of values
	 * @filter
	 */
	public static function dec($key, $values, $buckets = 'global', array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'values', 'buckets', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			$result = array();

			$values = (is_string($values)) ? array($values => 1): $values;
			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix)) ? Redis::addPrefix($val, $prefix) : $val;
				$result[$options['prefix']] = Redis::decrementHash($key, $values, $options);
			}
			return (count($result) > 1) ? $result : array_shift($result);
		});
	}

	/**
	 * Returns an array with all stats and their values in it
	 *
	 * If you want to retrieve stats for one specific bucket or dimension, you just pass in
	 * the key to identify which stats to retrieve, i.e. `Stats::get('requests')`. If you omit
	 * the bucket param at all, `global` will be used as a default.
	 *
	 * If you want to retrieve stats for requests but for a set of buckets, you can pass in
	 * additional buckets as an array:
	 *
	 * {{{
	 *	Stats::get('requests');
	 *	Stats::get('requests', 'bucket1');
	 *	Stats::get('requests', array('bucket1', 'bucket2'));
	 *	Stats::get('requests', array('user' => 'foo'));
	 *	Stats::get('requests', array('user' => 'foo', 'year' => date('Y')));
	 * }}}
	 *
	 * Additionally, you can limit the result to certain fields, to limit the size of arrays, by
	 * passing in a third parameter. That can be a string to retrieve only one field, or an array
	 * of fields.
	 *
	 * If you pass in only one bucket, you will retrieve that stats with an array keyed off by
	 * statname and the corresponding value. If you request more than one bucket at once, each
	 * stats-array will be prefixed by the corresponding redis-prefix for that stat, e.g.
	 * requesting stats for `user=foo` and `year=2013` you will get:
	 *
	 * {{{
	 *    [global] => Array(
	 *       [processed] => 1853
	 *       [finished] => 253
	 *    )
	 *    [user:foo] => Array(
	 *       [processed] => 53
	 *       [finished] => 38
	 *    )
	 *    [year:2013] => Array(
	 *       [processed] => 260
	 *       [finished] => 111
	 *   )
	 * }}}
	 *
	 * @see li3_redis\storage\Stats::inc()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key for which to retrieve stats for
	 * @param string|array $buckets string or array with additional buckets, see Stats::inc()
	 * @param string|array $fields a string or an array of fields to retrieve
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return array stats name is key and value is the corresponding value
	 * @filter
	 */
	public static function get($key, $buckets = 'global', $fields = '', array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'buckets', 'fields', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			$result = array();
			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix)) ? Redis::addPrefix($val, $prefix) : $val;
				$result[$options['prefix']] = Redis::readHash($key, $fields, $options);
			}
			return (count($result) > 1) ? $result : array_shift($result);
		});
	}

	/**
	 * sets given values
	 *
	 * {{{
	 *   Stats::set('foo', array('field' => 'value'));
	 *   Stats::set('foo', array('field1' => 'value1', 'field2' => 'value2'));
	 * }}}
	 *
	 * @see li3_redis\storage\Stats::inc()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key in which to store the hash
	 * @param array $values an array of values to store, the format is
	 *        array($name => $value) and can contain an unlimited number of stats to save.
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the new value that has been stored or an array of values
	 * @filter
	 */
	public static function set($key, $values, $buckets = 'global', array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'values', 'buckets', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			$result = array();

			$values = (is_string($values)) ? array($values => 1): $values;
			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix)) ? Redis::addPrefix($val, $prefix) : $val;
				$result[$options['prefix']] = Redis::writeHash($key, $values, $options);
			}
			return (count($result) > 1) ? $result : array_shift($result);
		});
	}

	/**
	 * delete given values from Stats, or a whole stats set at once
	 *
	 * {{{
	 *   Stats::delete('foo'); // will remove the whole set
	 *   Stats::delete('foo', 'field'); // will remove `field` from `foo`
	 *   Stats::delete('foo', array('field')); // same as above
	 *   Stats::delete('foo', array('field1', 'field2')); // will remove both fields from `foo`
	 * }}}
	 *
	 *
	 * @see li3_redis\storage\Stats::inc()
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key which identifies the hash
	 * @param string|array $fields a string to remove a field from given hash or an array thereof.
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return integer count of values that has been removed
	 * @filter
	 */
	public static function delete($key, $fields = null, $buckets = 'global', array $options = array()) {
		$defaults = array('namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'fields', 'buckets', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			$result = array();

			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix))
					? Redis::addPrefix($val, $prefix)
					: $val;
				if (empty($fields)) {
					$result[] = Redis::delete($key, $options);
				} else {
					$return = Redis::deleteFromHash($key, $fields, $options);
					$result[] = (is_numeric($return)) ? $return : array_sum($return);
				}
			}
			return array_sum($result);
		});
	}

	/**
	 * returns statistic values, determined by key and fields
	 *
	 * The main-purpose of this method is to have a quick way to access a certain stat-value. You
	 * can even request stat-values for a bucketed value, if you know the prefix and set that in
	 * options, see examples.
	 *
	 * The values will be returned as flat, as possible, meaning: if it is only one value, the value
	 * will be returned on its own. If you request more than one value, an array is returned.
	 *
	 * examples:
	 *
	 * {{{
	 *	Stats::values('api', 'calls');
	 *	Stats::values('api', array('calls', 'successful'));
	 *	Stats::values('api', array('calls', 'successful'), array('prefix' => 'foo'));
	 *	Stats::values('api', array('calls'), array('prefix' => 'foo', 'namespace' => 'bar'));
	 *	Stats::values('api', 'calls', array('prefix' => 'foo'));
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key which identifies the hash
	 * @param string|array $fields a string or an array of fields to retrieve
	 * @param array $options array with additional options, see Redis::getKey()
	 * @return mixed the value to be retrieved or an array of values
	 * @filter
	 */
	public static function values($key, $fields = '', array $options = array()) {
		$defaults = array('prefix' => 'global', 'namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'fields', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			return Redis::readHash($params['key'], $params['fields'], $params['options']);
		});
	}

	/**
	 * returns how many fields are at given `$key`
	 *
	 * {{{
	 *  Stats::length('requests');
	 *  Stats::length('requests', 'bucket1');
	 *  Stats::length('requests', array('bucket1', 'bucket2'));
	 *  Stats::length('requests', array('user' => 'foo'));
	 *  Stats::length('requests', array('user' => 'foo', 'year' => date('Y')));
	 *  Stats::length('requests', 'global', array('prefix' => 'bar');
	 *  Stats::length('requests', 'global', array('prefix' => 'bar', 'namespace' => 'baz'));
	 * }}}
	 *
	 * @see li3_redis\storage\Redis::getKey()
	 * @param string $key redis key which identifies the hash
	 * @param string|array $buckets an array of additional prefixes, can be a numerical indexed
	 *        array with strings, or an associated array, in which the key and value will be glued
	 *        together by a separater or just a string, for one additional prefix.
	 * @param array $options array with additional options, see Redis::getKey()
	 * @filter
	 */
	public static function length($key, $buckets = 'global', array $options = array()) {
		$defaults = array('prefix' => 'global', 'namespace' => static::$namespace);
		$options += $defaults;
		$params = compact('key', 'buckets', 'options');
		return static::_filter(__METHOD__, $params, function($self, $params) {
			extract($params);
			$result = array();

			$buckets = (!is_array($buckets))? array($buckets) : $buckets;

			foreach ($buckets as $prefix => $val) {
				$options['prefix'] = (!is_numeric($prefix)) ? Redis::addPrefix($val, $prefix) : $val;
				$result[$options['prefix']] = Redis::hashLength($key, $options);
			}
			return (count($result) > 1) ? $result : array_shift($result);
		});
	}
}

?>
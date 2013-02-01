<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

use lithium\data\Connections;

/**
 * connection to redis via li3_redis library
 */
Connections::add('li3_redis', array(
	'type' => 'Redis',
	'host' => '127.0.0.1:6379',
	'database' => 1,
	'persistent' => true,
	'expiry' => 0,
));

?>
<?php

use lithium\action\Dispatcher;
use li3_redis\storage\Redis;

/**
 * Apply filter to Dispatcher, to initialize Redis configuration
 */
Dispatcher::applyFilter('_call', function($self, $params, $chain) {
	Redis::config();
	return $chain->next($self, $params, $chain);
});

?>
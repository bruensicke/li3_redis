# li3_redis

Lithium library for using redis. Allows easy use of specific use cases with some advanced features.

## Requirements

You need the `phpredis` PHP extension, which can be found here:

[https://github.com/nicolasff/phpredis](https://github.com/nicolasff/phpredis)

An easy way to install it could be this:

	git clone git://github.com/nicolasff/phpredis.git
	cd phpredis
	phpize
	./configure
	make
	sudo -s make install
	sudo -s
	echo "extension=redis.so" > /etc/php5/conf.d/redis.ini
	exit
	sudo service apache2 restart

## Installation

Add a submodule to your li3 libraries:

	git submodule add git@github.com:bruensicke/li3_redis.git libraries/li3_redis

and activate it in you app (config/bootstrap/libraries.php), of course:

	Libraries::add('li3_redis');

## Todos

i want to address the following topics. If you need something or can share something of value, please get in touch with me.

- create redis datasource (done)
- use Connection class to define connections (done)
- fix failing unit-tests
- convert leaderboard class to use lithium standards `_init()` etc. (partly done)
- use Redis class as utility class for Leaderboard (done)
- auto-add/remove namespace formats to include environments or other variables in keys (done)
- add points / score class
- add user messaging system
- add basic queue system (that may also go into [li3_queue](https://github.com/bruensicke/li3_queue))

## Credits

* [li3](http://www.lithify.me)
* [PHP Leaderboard](https://github.com/agoragames/php-leaderboard).

Please report any bug or feature-request here: https://github.com/bruensicke/li3_redis/issues

<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\extensions\command;

use li3_redis\storage\Stats as StatsCore;

/**
 * Stas Command
 */
class Stats extends \lithium\console\Command {

	/**
	 * Auto run the help command.
	 *
	 * @param string $command Name of the stats to print on screen.
	 * @return void
	 */
	public function run($name = null) {
		if (!$name) {
			return $this->error('missing parameter $name');
			// $this->help();
		}
		$this->show($name);
	}

	public function show($name) {
		$stats = StatsCore::get($name);
		$max = max(array_map('strlen', array_keys($stats)));
		foreach ($stats as $key => $value) {
			$this->out(sprintf("%{$max}s: %s", $key, $value));
		}
	}


}


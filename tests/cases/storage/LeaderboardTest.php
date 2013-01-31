<?php
/**
 * li3_redis: redis made ready for lithium
 *
 * @copyright     Copyright 2013, brünsicke.com GmbH (http://bruensicke.com)
 * @license       http://opensource.org/licenses/BSD-3-Clause The BSD License
 */

namespace li3_redis\tests\cases\storage;

use li3_redis\storage\Leaderboard;
use li3_redis\storage\Redis;

use Redis as RedisCore;

class LeaderboardTest extends \lithium\test\Unit {

	public $redis;

	public function setUp() {
		$this->redis = new RedisCore();
		$this->redis->connect('127.0.0.1', 6379);
		$this->redis->select(1);
		$this->redis->flushDB();
		$namespace = Leaderboard::$namespace;
		$this->prefix = Redis::resolveFormat(null, compact('namespace')).':';
		Redis::connection()->select(1);
	}

	public function tearDown() {
		$this->redis->select(1);
		$this->redis->flushDB();
	}

	function testConstructLeaderboardClassWithName() {
		$leaderboard = new Leaderboard('leaderboard');
		$this->assertEqual('leaderboard', $leaderboard->getName());
		$namespace = Leaderboard::$namespace;
		$expected = Redis::getKey('leaderboard', compact('namespace'));
		$this->assertEqual($expected, $leaderboard->getKey());
	}

	function testCloseLeaderboardConnection() {
		$leaderboard = new Leaderboard('leaderboard');
		$this->assertTrue($leaderboard->close());
	}

	function testAddMember() {
		$leaderboard = new Leaderboard('leaderboard');
		$leaderboard->removeMember('david');
		$this->assertEqual(1, $leaderboard->addMember('david', 69));
		$this->assertEqual(1, $this->redis->zSize($this->prefix.'leaderboard'));
	}

	function testRemoveMember() {
		$leaderboard = new Leaderboard('leaderboard');
		$this->assertEqual(1, $leaderboard->addMember('david', 69));
		$this->assertEqual(1, $leaderboard->removeMember('david'));
		$this->assertEqual(0, $this->redis->zSize($this->prefix.'leaderboard'));
	}

	function testTotalMembers() {
		$leaderboard = new Leaderboard('leaderboard');
		$this->assertEqual(1, $leaderboard->addMember('david', 69));
		$this->assertEqual(1, $leaderboard->totalMembers());
	}

	function testTotalPages() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= Leaderboard::DEFAULT_PAGE_SIZE + 1; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$this->assertEqual(2, $leaderboard->totalPages());
	}

	function testSetPageSize() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= Leaderboard::DEFAULT_PAGE_SIZE + 5; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$this->assertEqual(2, $leaderboard->totalPages());
		$leaderboard->setPageSize(10);
		$this->assertEqual(3, $leaderboard->totalPages());
	}


	function testTotalMembersInScoreRange() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= Leaderboard::DEFAULT_PAGE_SIZE + 1; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$this->assertEqual(3, $leaderboard->totalMembersInScoreRange(2, 4));
	}

	function testChangeScoreFor() {
		$leaderboard = new Leaderboard('leaderboard');

		$leaderboard->changeScoreFor('member_1', 5);
		$this->assertEqual(5, $leaderboard->scoreFor('member_1'));

		$leaderboard->changeScoreFor('member_1', 5);
		$this->assertEqual(10, $leaderboard->scoreFor('member_1'));

		$leaderboard->changeScoreFor('member_1', -5);
		$this->assertEqual(5, $leaderboard->scoreFor('member_1'));
	}

	function testCheckMember() {
		$leaderboard = new Leaderboard('leaderboard');

		$leaderboard->addMember('member_1', 10);
		$this->assertTrue($leaderboard->checkMember('member_1'));
		$this->assertFalse($leaderboard->checkMember('member_2'));
	}

	function testRankFor() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= Leaderboard::DEFAULT_PAGE_SIZE + 1; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$this->assertEqual(26, $leaderboard->rankFor('member_1'));
		$this->assertEqual(25, $leaderboard->rankFor('member_1', true));
	}

	function testScoreFor() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= Leaderboard::DEFAULT_PAGE_SIZE + 1; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$this->assertEqual(14, $leaderboard->scoreFor('member_14'));
	}

	function testScoreAndRankFor() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= 5; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$memberData = $leaderboard->scoreAndRankFor('member_1');
		$this->assertEqual('member_1', $memberData['member']);
		$this->assertEqual(1, $memberData['score']);
		$this->assertEqual(5, $memberData['rank']);
	}

	function testRemoveMembersInScoreRange() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= 5; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$this->assertEqual(5, $leaderboard->totalMembers());

		$leaderboard->addMember('cheater_1', 100);
		$leaderboard->addMember('cheater_2', 101);
		$leaderboard->addMember('cheater_3', 102);

		$this->assertEqual(8, $leaderboard->totalMembers());

		$leaderboard->removeMembersInScoreRange(100, 102);

		$this->assertEqual(5, $leaderboard->totalMembers());

		$leaders = $leaderboard->leaders(1);
		foreach ($leaders as $key => $value) {
			$this->assertTrue((100 > $value['score']));
		}
	}

	function testLeaders() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= Leaderboard::DEFAULT_PAGE_SIZE + 1; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$leaders = $leaderboard->leaders(1);
		$this->assertEqual(Leaderboard::DEFAULT_PAGE_SIZE, count($leaders));
		$this->assertEqual('member_26', $leaders[0]['member']);
		$this->assertEqual(26, $leaders[0]['score']);
		$this->assertEqual(1, $leaders[0]['rank']);

		$leaders = $leaderboard->leaders(2);
		$this->assertEqual(1, count($leaders));
		$this->assertEqual('member_1', $leaders[0]['member']);
		$this->assertEqual(1, $leaders[0]['score']);
		$this->assertEqual(26, $leaders[0]['rank']);
	}

	function testAroundMe() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= Leaderboard::DEFAULT_PAGE_SIZE * 3 + 1; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$this->assertEqual(Leaderboard::DEFAULT_PAGE_SIZE * 3 + 1, $leaderboard->totalMembers());

		$leadersAroundMe = $leaderboard->aroundMe('member_30');
		$this->assertEqual(Leaderboard::DEFAULT_PAGE_SIZE / 2, count($leadersAroundMe) / 2);

		$leadersAroundMe = $leaderboard->aroundMe('member_1');
		$this->assertEqual(ceil(Leaderboard::DEFAULT_PAGE_SIZE / 2 + 1), count($leadersAroundMe));

		$leadersAroundMe = $leaderboard->aroundMe('member_76');
		$this->assertEqual(Leaderboard::DEFAULT_PAGE_SIZE / 2, count($leadersAroundMe) / 2);
	}

	function testRankedInList() {
		$leaderboard = new Leaderboard('leaderboard');
		for ($i = 1; $i <= Leaderboard::DEFAULT_PAGE_SIZE; $i++) {
			$leaderboard->addMember("member_{$i}", $i);
		}

		$this->assertEqual(Leaderboard::DEFAULT_PAGE_SIZE, $leaderboard->totalMembers());
		$members = array('member_1', 'member_5', 'member_10');
		$rankedMembers = $leaderboard->rankedInList($members);

		$this->assertEqual(3, count($rankedMembers));

		$this->assertEqual(25, $rankedMembers[0]['rank']);
		$this->assertEqual(1, $rankedMembers[0]['score']);

		$this->assertEqual(21, $rankedMembers[1]['rank']);
		$this->assertEqual(5, $rankedMembers[1]['score']);

		$this->assertEqual(16, $rankedMembers[2]['rank']);
		$this->assertEqual(10, $rankedMembers[2]['score']);
	}
}

?>
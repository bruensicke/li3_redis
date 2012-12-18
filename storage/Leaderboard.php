<?php

namespace li3_redis\storage;

use li3_redis\storage\Redis;

/**
 * Leaderboards are a redis based sorted set that allow easy storage and retrieval of players scores
 *
 * It implements the logic derived from agora games leaderboards. For further information about
 * usage and the internals have a look at:
 *
 * https://github.com/agoragames/leaderboard
 * http://blog.agoragames.com/blog/2011/01/01/creating-high-score-tables-leaderboards-using-redis/
 * http://redis.io/commands#sorted_set
 *
 * The basic implementation is as described in the documentation but there are additional methods
 * available for your gaming leadboards pleasure.
 *
 * @see https://github.com/agoragames/leaderboard
 */
class Leaderboard {

	const DEFAULT_PAGE_SIZE = 100;

	public static $namespace = 'leaderboards';

	public $name; // public name
	private $connection;
	private $_namespace;
	private $_page_size;

	public function __construct($name, $namespace = null) {
		$this->name = $name;
		$this->connection = Redis::connection();
		$this->setNamespace($namespace);
	}

	public function getNamespace() {
		if (!empty($this->_namespace)) {
			return $this->_namespace;
		}
		return Leaderboard::$namespace;
	}

	public function setNamespace($namespace = null) {
		$namespace = ($namespace) ? : Leaderboard::$namespace;
		return $this->_namespace = $namespace;
	}

	public function getKey($name = null) {
		$name = $name ? : $this->name;
		return Redis::addKey($name, $this->getNamespace());
	}

	public function getName() {
		return $this->name;
	}

	public function setPageSize($pageSize) {
		if ($pageSize < 1) {
			$pageSize = Leaderboard::DEFAULT_PAGE_SIZE;
		}
		return $this->_page_size = $pageSize;
	}

	public function close() {
		return true;
	}

	public function addMember($member, $score) {
		return $this->addMemberTo($this->name, $member, $score);
	}

	public function addMemberTo($name, $member, $score) {
		return $this->connection->zAdd($this->getKey($name), $score, $member);
	}

	public function removeMember($member) {
		return $this->removeMemberFrom($this->name, $member);
	}

	public function removeMemberFrom($name, $member) {
		return $this->connection->zRem($this->getKey($name), $member);
	}

	public function totalMembers() {
		return $this->totalMembersIn($this->name);
	}

	public function totalMembersIn($name) {
		return $this->connection->zCard($this->getKey($name));
	}

	public function totalPages() {
		return $this->totalPagesIn($this->name);
	}

	public function totalPagesIn($name) {
		return ceil($this->totalMembersIn($name) / $this->_page_size);
	}

	public function totalScore() {
		return $this->totalScoreIn($this->name);
	}

	public function totalScoreIn($name) {
		return array_sum($this->allMembersIn($name));
	}

	public function allMembers() {
		return $this->allMembersIn($this->name);
	}

	public function allMembersIn($name) {
		return $this->connection->zRevRange($this->getKey($name), 0, -1, true);
	}

	public function totalMembersInScoreRange($minScore, $maxScore) {
		return $this->totalMembersInScoreRangeIn($this->name, $minScore, $maxScore);
	}

	public function totalMembersInScoreRangeIn($name, $minScore, $maxScore) {
		return $this->connection->zCount($this->getKey($name), $minScore, $maxScore);
	}

	public function changeScoreFor($member, $delta) {
		return $this->changeScoreForIn($this->name, $member, $delta);
	}

	public function changeScoreForIn($name, $member, $delta) {
		return $this->connection->zIncrBy($this->getKey($name), $delta, $member);
	}

	public function rankFor($member, $useZeroIndexForRank = false) {
		return $this->rankForIn($this->name, $member, $useZeroIndexForRank);
	}

	public function rankForIn($name, $member, $useZeroIndexForRank = false) {
		if ($this->connection->zScore($this->getKey($name), $member) == null) {
			return false;
		}
		$rank = $this->connection->zRevRank($this->getKey($name), $member);
		if ($useZeroIndexForRank == false) {
			$rank += 1;
		}

		return $rank;
	}

	public function scoreFor($member) {
		return $this->scoreForIn($this->name, $member);
	}

	public function scoreForIn($name, $member) {
		return $this->connection->zScore($this->getKey($name), $member);
	}

	public function checkMember($member) {
		return $this->checkMemberIn($this->name, $member);
	}

	public function checkMemberIn($name, $member) {
		return !($this->connection->zScore($this->getKey($name), $member) == null);
	}

	public function scoreAndRankFor($member, $useZeroIndexForRank = false) {
		return $this->scoreAndRankForIn($this->name, $member, $useZeroIndexForRank);
	}

	public function scoreAndRankForIn($name, $member, $useZeroIndexForRank = false) {
		$memberData = array();
		$memberData['member'] = $member;
		$memberData['score'] = $this->scoreForIn($name, $member);
		$memberData['rank'] = $this->rankForIn($name, $member, $useZeroIndexForRank);

		return $memberData;
	}

	public function removeMembersInScoreRange($minScore, $maxScore) {
		return $this->removeMembersInScoreRangeIn($this->name, $minScore, $maxScore);
	}

	public function removeMembersInScoreRangeIn($name, $minScore, $maxScore) {
		return $this->connection->zRemRangeByScore($this->getKey($name), $minScore, $maxScore);
	}

	public function leaders($currentPage, $withScores = true, $withRank = true, $useZeroIndexForRank = false) {
		return $this->leadersIn($this->name, $currentPage, $withScores, $withRank, $useZeroIndexForRank);
	}

	public function leadersIn($name, $currentPage, $withScores = true, $withRank = true, $useZeroIndexForRank = false) {
		if ($currentPage < 1) {
			$currentPage = 1;
		}

		if ($currentPage > $this->totalPagesIn($name)) {
			$currentPage = $this->totalPagesIn($name);
		}

		$indexForRedis = $currentPage - 1;

		$startingOffset = ($indexForRedis * $this->_page_size);
		if ($startingOffset < 0) {
			$startingOffset = 0;
		}

		$endingOffset = ($startingOffset + $this->_page_size) - 1;

		$leaderData = $this->connection->zRevRange($this->getKey($name), $startingOffset, $endingOffset, $withScores);
		if (!is_null($leaderData)) {
			return $this->massageLeaderData($name, $leaderData, $withScores, $withRank, $useZeroIndexForRank);
		} else {
			return null;
		}
	}

	public function aroundMe($member, $withScores = true, $withRank = true, $useZeroIndexForRank = false) {
		return $this->aroundMeIn($this->name, $member, $withScores, $withRank, $useZeroIndexForRank);
	}

	public function aroundMeIn($name, $member, $withScores = true, $withRank = true, $useZeroIndexForRank = false) {
		$reverseRankForMember = $this->connection->zRevRank($this->getKey($name), $member);

		$startingOffset = $reverseRankForMember - ($this->_page_size / 2);
		if ($startingOffset < 0) {
			$startingOffset = 0;
		}

		$endingOffset = ($startingOffset + $this->_page_size) - 1;

		$leaderData = $this->connection->zRevRange($this->getKey($name), $startingOffset, $endingOffset, $withScores);
		if (!is_null($leaderData)) {
			return $this->massageLeaderData($name, $leaderData, $withScores, $withRank, $useZeroIndexForRank);
		}
		return null;
	}

	public function rankedInList($members, $withScores = true, $useZeroIndexForRank = false) {
		return $this->rankedInListIn($this->name, $members, $withScores, $useZeroIndexForRank);
	}

	public function rankedInListIn($name, $members, $withScores = true, $useZeroIndexForRank = false) {
		$leaderData = array();

		foreach ($members as $member) {
			$memberData = array();
			$memberData['member'] = $member;
			if ($withScores) {
				$memberData['score'] = (float) $this->scoreForIn($name, $member);
			}
			$memberData['rank'] = $this->rankForIn($name, $member, $useZeroIndexForRank);

			array_push($leaderData, $memberData);
		}

		return $leaderData;
	}

	public function scoredInList($from, $to, $withScores = true) {
		return $this->scoredInListIn($this->name, $from, $to, $withScores);
	}

	public function scoredInListIn($name, $from, $to, $withScores = true) {
		return $this->connection->zRangeByScore($this->getKey($name), $from, $to, array('withscores' => $withScores));
	}

	private function massageLeaderData($name, $leaders, $withScores, $withRank, $useZeroIndexForRank) {
		$memberAttribute = true;
		$leaderData = array();

		$memberData = array();
		foreach ($leaders as $key => $value) {

			$memberData['member'] = $key;
			$memberData['score'] = (float) $value;

			if ($withRank) {
				$memberData['rank'] = $this->rankForIn($name, $key, $useZeroIndexForRank);
			}

			array_push($leaderData, $memberData);
			$memberData = array();
		}

		return $leaderData;
	}
}

?>
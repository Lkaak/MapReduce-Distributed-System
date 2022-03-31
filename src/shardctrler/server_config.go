package shardctrler

func (sc *ShardCtrler) MakeJoinConfig(servers map[int][]string) Config {
	//函数外面已经加锁，所以不需要再加锁
	//获取以前的config信息
	oldConfig := sc.configs[len(sc.configs)-1]
	//利用新加的服务器和以前的group信息生成新的Group
	newConfigGroup := make(map[int][]string)
	//读取旧的Group
	for gid, server := range oldConfig.Groups {
		newConfigGroup[gid] = server
	}
	//读取新的Group
	for gid, server := range servers {
		newConfigGroup[gid] = server
	}
	//读取现有的shard与group的关系
	gidsToShards := make(map[int]int) //shard->groupNum
	//全部初始化为0
	for gid := range newConfigGroup {
		gidsToShards[gid] = 0
	}
	//读取旧的shard信息
	for _, gid := range oldConfig.Shards {
		//注意可能删除了部分shards或者刚刚初始化都为0
		if gid != 0 {
			gidsToShards[gid] += 1
		}
	}
	//生产新的config，重新均匀分配shard和group的关系
	return Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(gidsToShards, oldConfig.Shards),
		Groups: newConfigGroup,
	}
}

func (sc *ShardCtrler) MakeLeaveConfig(gids []int) Config {
	//函数外面已经加锁，所以不需要再加锁
	//获取以前的config信息
	oldConfig := sc.configs[len(sc.configs)-1]
	//利用新加的服务器和以前的group信息生成新的Group
	newConfigGroup := make(map[int][]string)
	//标注需删除的gid
	needleave := make(map[int]bool)
	for _, gid := range gids {
		needleave[gid] = true
	}
	//读取旧的Group
	for gid, server := range oldConfig.Groups {
		newConfigGroup[gid] = server
	}
	//生成新的group
	for _, gid := range gids {
		delete(newConfigGroup, gid)
	}
	//读取现有的shard与group的关系
	gidsToShards := make(map[int]int) //shard->groupNum
	//全部初始化为0
	for gid := range newConfigGroup {
		gidsToShards[gid] = 0
	}
	//读取旧的shard信息
	for shard, gid := range oldConfig.Shards {
		//注意为0则表示没有对应的gid，即没有对应关系
		if gid != 0 {
			if needleave[gid] {
				oldConfig.Shards[shard] = 0
			} else {
				gidsToShards[gid] += 1
			}
		}
	}
	//生产新的config，重新均匀分配shard和group的关系
	//注意可能所有group都走了，则无法执行重新分配
	if len(newConfigGroup) == 0 {
		return Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newConfigGroup,
		}
	}
	return Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(gidsToShards, oldConfig.Shards),
		Groups: newConfigGroup,
	}
}

func (sc *ShardCtrler) MakeMoveConfig(movegid, moveshard int) Config {
	oldConfig := sc.configs[len(sc.configs)-1]
	oldConfig.Num += 1
	oldConfig.Shards[moveshard] = movegid
	return oldConfig
}

func (sc *ShardCtrler) reBalanceShards(gidsToShards map[int]int, oldShards [NShards]int) [NShards]int {
	groupNum := len(gidsToShards)   //group个数
	average := NShards / groupNum   //每个group的shard的平均个数
	remainder := NShards % groupNum //余数
	DPrintf("")
	//排序后确保从后往前遍历时，可以先遍历到拥有多的分片的gid让其先退分片
	//被退出来的分片则可以用于分配给排序在前面的新加入的分组
	sortGids := SortGids(gidsToShards)
	DPrintf("-----sortres:%v", sortGids)
	//修改以前的shards
	//从后往前遍历，确保先看到有多的从而能先退给补留出空闲的
	for i := groupNum - 1; i >= 0; i-- {
		//应该分得的分片数
		resNum := average
		//把余数分给后面的
		if !ifAvg(groupNum, remainder, i) {
			resNum += 1
		}
		//多退
		if resNum < gidsToShards[sortGids[i]] {
			fromGid := sortGids[i]
			changNum := gidsToShards[sortGids[i]] - resNum
			for shard, gid := range oldShards {
				if changNum <= 0 {
					break
				}
				if gid == fromGid {
					oldShards[shard] = 0
					changNum -= 1
				}
			}
			//gidsToShards[sortGids[i]]=resNum,因为按照顺序只遍历了一次，所以不用更新已经遍历过的旧的映射关系
		}
		//少补（新加入的group）
		if resNum > gidsToShards[sortGids[i]] {
			toGid := sortGids[i]
			changNum := resNum - gidsToShards[sortGids[i]]
			for shard, gid := range oldShards {
				if changNum <= 0 {
					break
				}
				if gid == 0 {
					oldShards[shard] = toGid
					changNum -= 1
				}
			}
		}
	}
	DPrintf("reBalanceShards result:%v", oldShards)
	return oldShards
}

func ifAvg(groupNum, remainder, i int) bool {
	if i < groupNum-remainder {
		return true
	} else {
		return false
	}

}

//按照拥有的分片数从小到大对gid排序,分片数相同的gid小的排前面
//这里必须严格排序，确保每个服务器不同顺序的输入的结果都是一致的，才能确保所有服务器的存储的config的结果一致
func SortGids(gidsToShards map[int]int) []int {
	length := len(gidsToShards)

	res := make([]int, 0, length)
	for gid, _ := range gidsToShards {
		res = append(res, gid)
	}
	//冒泡排序，保证按照分得的shard数量从小到大排序
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if gidsToShards[res[j]] < gidsToShards[res[j-1]] || gidsToShards[res[j]] == gidsToShards[res[j-1]] && res[j] < res[j-1] {
				res[j], res[j-1] = res[j-1], res[j]
			}
		}
	}
	return res
}

import heapq
from itertools import permutations


def closest(lst, K):
    return heapq.nsmallest(1, lst, key=lambda x: abs(x - K))[0]


userNum = int(input())
list1 = []
outlist = []
checklist = []
list2 = []

for i in range(userNum):
    list1.append(int(input()))

for i in range(len(list1)):
    outlist.append(list(permutations(list1, i + 1)))

for lists in outlist:
    for items in lists:
        total = 0
        for nums in items:
            total += int(nums)
    checklist.append(total)

set(checklist)

for items in checklist:
    if items == 1000:
        print(1000)
        break
    elif items > 1000:
        del checklist[0:(checklist.index(items) - 2)]
        del checklist[(checklist.index(items) + 1):]

print(closest(checklist, 1000)))
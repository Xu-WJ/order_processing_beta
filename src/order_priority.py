# -*- coding: utf-8 -*-

'''
Order priority setup
'''

#Order type and priority
ORDER_PRI = {}
#Consider the following types of orders: type represent id in type string!

SINGLE = 0   ;  ORDER_PRI[SINGLE] = 9     # single item order
NO_MERGE = 1 ;  ORDER_PRI[NO_MERGE] = 8   # multiple item order with no merge
MERGE = 2    ;  ORDER_PRI[MERGE] = 7      # multiple item order with merge
SPEC = 3     ;  ORDER_PRI[SPEC] = 6       # special order
BACK = 4     ;  ORDER_PRI[BACK] = 5       # return to supplier
SPARE =5     ;  ORDER_PRI[SPARE] = 4      # ship to spare unit warehouse
BULK = 6     ;  ORDER_PRI[BULK] = 3       # bulk order
MOVE = 7     ;  ORDER_PRI[MOVE] = 2       # move 
INT = 8      ;  ORDER_PRI[INT] = 1        # internal order
JZD = 9      ;  ORDER_PRI[JZD] = 10       # JD on-time delivery
FOO = 10     ;  ORDER_PRI[FOO] = 11       # 411 order


# parameter used in pre-screening
THETA = 0.3 

# parellel processing calculating R
NProcessing = 5

# threshold for S
thresholdS = 0.5

# slot volume waste upper bound
volumeWasteRatioUpper = 0.5

# order last fail time gap
lastFailGapUpper = 300 # 5 minutes

# zudan outmost cycle max
outmost_cycle_count_max = 1000

# paichan: alpha for objective functions
ALPHA = {1: 1, 2: 1, 3: 10}
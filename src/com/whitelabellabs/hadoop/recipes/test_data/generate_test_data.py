#!/usr/bin/env python

import random
dates_to_generate = ['2010_01_01',
                     '2010_01_02',
                     '2010_01_03',
                     '2010_01_04',
                     '2010_01_05',
                     '2010_01_06',
                     '2010_01_07',
                     '2010_01_08',
                     '2010_01_09',
                     '2010_01_10',
                     '2010_01_11',
                     '2010_01_12',
                     '2010_01_13',
                     '2010_01_14',
                     '2010_01_15',
                     ]

num_entries_to_generate = 1000
num_permutations = 10
items = ['user_id_',
         'content_id_',
         'ip_',
         ]


for date in dates_to_generate:
    f = open(date, 'w')
    for i in xrange(num_entries_to_generate):
        f.write(','.join([ x+ str(random.randint(0,num_permutations))for x in items]))
        f.write('\n')
        
    f.close()
    num_permutations += 1

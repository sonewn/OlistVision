# import os
# import pandas as pd

# for dirname, _, filenames in os.walk('/Users/soy/GitProjects/OlistVision/bucket/olist_source'):
#     for filename in filenames:
#         fn = filename.split('.csv')[0].split('_dataset')[0]
#         globals()[fn] = pd.read_csv(os.path.join(dirname, filename))
#         print(f"{fn}'s shape is ", globals()[fn].shape)
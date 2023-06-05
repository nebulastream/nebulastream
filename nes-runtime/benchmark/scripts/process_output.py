import pandas as pd
import sys
# Read the csv file
file_path = sys.argv[1]
df = pd.read_csv(file_path, delimiter=',')

# Group by 'Scale Factor' and 'Compiler'

grouped = df.groupby(['ScaleFactor'])

# Get unique 'Scale Factor' values
scale_factors = df['ScaleFactor'].unique()

# Loop through each 'Scale Factor'
for factor in scale_factors:
    # Get each subgroup
    for name, group in grouped:
        if name[0] == factor:
            group['RunTime'] =  group['ExecutionTime'] + group['CompileTime']
            group['QueryPerS'] =  1000 / group['RunTime']
            pivot_df = group.pivot(index='Compiler', columns='Query', values='QueryPerS')
            # Write each subgroup to a new csv file
            pivot_df.to_csv(f'scale_factor_{factor}.csv', index=True)

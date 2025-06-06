# python
import sys, os, glob
import pandas as pd
import seaborn as sns

def create_plots(path):
    # prepare output folder
    fig_dir = os.path.join(os.path.dirname(__file__), "fig", os.path.basename(path))
    os.makedirs(fig_dir, exist_ok=True)

    df = pd.read_csv(path)

    # 1. Total time per query / layout / buffer
    g1 = sns.catplot(
        data=df.drop_duplicates(['query','memory_layout','buffer_bytes','total_time_s']),
        x='query', y='total_time_s',
        hue='memory_layout', col='buffer_bytes',
        kind='bar', height=4, aspect=1
    )
    g1.set_xticklabels(rotation=45, ha='right')
    g1.fig.suptitle('Total time per Query by Layout and Buffer', y=1.02)
    g1.savefig(os.path.join(fig_dir, 'total_time_per_query.png'), bbox_inches='tight')

    # 2. Pipeline 2 throughput comparison
    p2 = df[df.pipeline_id == 2] #y='throughput',
    g2 = sns.catplot(
        data=p2, x='query', y= 'tuples_per_second',
        hue='memory_layout', col='buffer_bytes',
        kind='bar', height=4, aspect=1
    )
    g2.set_xticklabels(rotation=45, ha='right')
    g2.fig.suptitle('Pipeline 2 Throughput per Query', y=1.02)
    g2.savefig(os.path.join(fig_dir, 'pipeline2_throughput.png'), bbox_inches='tight')

    # add pipeline labels
    pipeline_map = {
        2: 'filter',
        3: 'inputFormatter',
        4: 'col->row',
        5: 'row->col'
    }
    df['pipeline_label'] = df['pipeline_id'].map(pipeline_map)

    # 3. Pipeline breakdown as % of total time
    g3 = sns.catplot(
        data=df,
        x='query', y='pct_of_total',
        hue='pipeline_label', col='buffer_bytes',
        kind='bar', height=4, aspect=1
    )
    g3.set_xticklabels(rotation=45, ha='right')
    g3.set_axis_labels('Query', 'Percent of Total Time')
    g3.set_titles('Buffer: {col_name}')
    g3.fig.suptitle('Pipeline Time Breakdown as % of Total Query Time', y=1.02)
    g3.savefig(os.path.join(fig_dir, 'pipeline_time_breakdown.png'), bbox_inches='tight')

def main(path):
    files = [path] if os.path.isfile(path) else glob.glob(os.path.join(path, '**', '*.csv'), recursive=True)
    for file in files:
        create_plots(file)
        print(f"Created plots for {file} in {os.path.join(os.path.dirname(__file__), 'fig')}")

if __name__=='__main__':
    if len(sys.argv)!=2:
        print(f"Usage: python {os.path.basename(__file__)} results_dataSet.csv")
        sys.exit(1)
    main(sys.argv[1])
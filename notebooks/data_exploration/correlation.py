import polars as pl
import plotly.express as px
import wandb


api = wandb.Api()
artifact = api.artifact("flood-forecasting/flood-dataset-missouri:latest")
artifact_dir = artifact.download()

df = pl.read_parquet(f"{artifact_dir}/flood_model_missouri.parquet").to_pandas()


df_new = df

numeric_cols = df_new.select_dtypes(include=['number']).columns.tolist()
corr_df = df_new[numeric_cols].corr()
corr_df = corr_df.dropna(how='all').dropna(axis=1, how='all')
print(corr_df)

corr_pd = corr_df

n_vars = len(corr_pd.columns)

fig_size = max(600, n_vars * 25)

fig = px.imshow(
    corr_pd,
    color_continuous_scale="RdBu_r",
    zmin=-1,
    zmax=1,
    aspect="auto"
)

fig.update_layout(
    title="Correlation Heatmap",
    width=fig_size,
    height=fig_size,
    xaxis=dict(
        tickangle=45,
        automargin=True
    ),
    yaxis=dict(
        automargin=True
    )
)

fig.update_xaxes(side="bottom")

fig.show()
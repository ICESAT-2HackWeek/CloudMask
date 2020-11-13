import panel as pn
import panel.widgets as pnw
pn.extension('plotly')
import plotly.express as px


def atl06_3D(data):
    """
    3D interactive scatter: filter using flags (cloud, blowing snow, quality) 
    and difference in height with ArcticDEM
    
    """
    # Static plot
    def static_3D(data=data, c0=1, c1=(0,1), c2=(0,1), c3=1, c4=(0,1), col=None):
        data['date']=data.time.dt.date.astype(str)
        d = data.loc[(data['start_rgt']==c0) &
                     (data['cloud_flg_asr'].between(c1[0], c1[1])) & 
                     (data['bsnow_conf'].between(c2[0], c2[1])) & 
                     (data['atl06_quality_summary']<=c3) & 
                     (data['height_diff'].between(c4[0], c4[1]))]
        
        return(px.scatter_3d(d, x='longitude', y='latitude', z='h_li', color=col, color_continuous_scale=px.colors.sequential.Viridis).
               update_traces(marker={"size": 1, "opacity": 0.6}).
               update_layout(autosize=False, width=800, height=800))
    
    # Widgets
    c0 = pnw.Select(name='start_rgt', 
                    value=data.start_rgt.min(), 
                    options=sorted(data.start_rgt.unique()))
    c1 = pnw.RangeSlider(name='cloud_flg_asr', 
                            start=data.cloud_flg_asr.min(), 
                            end=data.cloud_flg_asr.max(), 
                            value=(data.cloud_flg_asr.min(), data.cloud_flg_asr.max()), 
                            step=1)
    c2 = pnw.RangeSlider(name='bsnow_conf',
                         start=data.bsnow_conf.min(), 
                         end=data.bsnow_conf.max(), 
                         value=(data.bsnow_conf.min(), data.bsnow_conf.max()), 
                         step=1)
    c3 = pnw.DiscreteSlider(name='atl06_quality_summary', 
                            value=data.atl06_quality_summary.max(), 
                            options=[0,1])
    c4 = pnw.RangeSlider(name='height_diff', 
                         start=data.height_diff.min(),
                         end=data.height_diff.max(), 
                         value=(data.height_diff.min(), data.height_diff.max()), 
                         step=1)
    col = pnw.Select(name='color', 
                     value='date', 
                     options=['h_li', 'date', 'ground_track'])
    
    # Interactive plot
    @pn.depends(c0, c1, c2, c3, c4, col)
    def reactive(c0, c1, c2, c3, c4, col):
        return(static_3D(data, c0, c1, c2, c3, c4, col))
    widgets = pn.Column(c0, col, c1, c2, c3, c4)
    image = pn.Row(reactive, widgets)
    
    return(image)
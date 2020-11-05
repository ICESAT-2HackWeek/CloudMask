import holoviews as hv
from holoviews import dim, opts
import panel as pn
import panel.widgets as pnw
hv.extension('plotly')

def atl06_3D(data):
    """
    3D interactive scatter: filter using flags (cloud, blowing snow, quality) 
    and difference in height with ArcticDEM
    
    """
    def static_3D(data=data, c1=(0,1), c2=(0,1), c3=1, c4=(0,1)):
        d = data.loc[(data['c_flg_asr'].between(c1[0], c1[1])) & 
                     (data['blowing_snow_conf'].between(c2[0], c2[1])) & 
                     (data['q_flag']<=c3) & 
                     (data['height_diff'].between(c4[0], c4[1]))]
        return(hv.Scatter3D((d.lon, d.lat, d.height)).opts(
        opts.Scatter3D(color='z', size=1, cmap='fire', width=900, height=800)))
    
    c1 = pnw.RangeSlider(name='cloud_flag', 
                            start=data.c_flg_asr.min(), 
                            end=data.c_flg_asr.max(), 
                            value=(data.c_flg_asr.min(), data.c_flg_asr.max()), 
                            step=1)
    c2 = pnw.RangeSlider(name='blowing_snow',
                         start=data.blowing_snow_conf.min(), 
                         end=data.blowing_snow_conf.max(), 
                         value=(data.blowing_snow_conf.min(), data.blowing_snow_conf.max()), 
                         step=1)
    c3 = pnw.DiscreteSlider(name='q_flag', 
                            value=data.q_flag.max(), 
                            options=[0,1])
    c4 = pnw.RangeSlider(name='height_diff', 
                         start=data.height_diff.min(),
                         end=data.height_diff.max(), 
                         value=(data.height_diff.min(), data.height_diff.max()), 
                         step=1)
    #c5 = pnw.Select(name='color', value='data.height', options=['data.height', 'data.time.dt.month'])
    @pn.depends(c1, c2, c3, c4)
    def reactive(c1, c2, c3, c4):
        return(static_3D(data, c1, c2, c3, c4))
    widgets = pn.Column(c1, c2, c3, c4)
    image = pn.Row(reactive, widgets)
    
    return(image)
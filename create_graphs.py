"""module for containing the code that produces charts"""
import os
from bokeh.charts import Bar, output_file, show
from bokeh.models import HoverTool


# bar chart showing total response by HH group split by digital/paper
def bar_response(results_list, output_path):

    output_dir = os.path.join(output_path, "charts")
    if os.path.isdir(output_dir) is False:
        os.mkdir(output_dir)

    tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

    for df in results_list:
        print(df)
        p = Bar(df, label='hh_type', values='perc_res', stack='digital', title="a_title",
                legend='top_right', tools=tools)

        hover = p.select_one(HoverTool)
        hover.point_policy = "follow_mouse"
        hover.tooltips = [
            ("count", "@height"),
        ]
        output_file_path = os.path.join(output_dir, 'test bar.html')
        output_file(output_file_path)
        show(p)



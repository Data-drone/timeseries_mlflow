#
# A function to explore a univariate series and generate an exploratory notebook
#
#
#


import nbformat as nbf
import inspect # To clean up indents

def create_notebook_exploration(setup_data = 'setup.py', 
                                output_name = 'test.ipynb'):
    """
    
    A Function to create a programmatic data exploration notebook

    We have specified a specific kernel spec that matches up with testing env

    current assumptions:
        Dataframes: train, val 

    """

    nb = nbf.v4.new_notebook()

    ############### First Section #######################
    text = inspect.cleandoc("""# Automated Exploration Notebook \n
        This is an automatically generate notebook""")

    import_spark_setup = "%run {0}".format(setup_data)

    show_train_data = "train.head()"

    ############# cells to explore data #################
    explore_heading = "## View"

    explore_dataset = inspect.cleandoc("""
        # Using plotly.express
        import plotly.express as px
        
        fig = px.line(train, x=train.index, y="total_rides")
        fig.show()
        """)


    nb['cells'] = [nbf.v4.new_markdown_cell(text),
                   nbf.v4.new_code_cell(import_spark_setup),
                   nbf.v4.new_code_cell(show_train_data),
                   nbf.v4.new_markdown_cell(explore_heading),
                   nbf.v4.new_code_cell(explore_dataset)]

    nb.metadata.kernelspec = {
        "display_name": "Python [conda env:spark]",
        "language": "python",
        "name": "conda-env-spark-py"
    }
    
    nbf.write(nb, output_name)


if __name__ == '__main__':
    create_notebook_exploration()
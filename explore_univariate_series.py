#
# A function to explore a univariate series and generate an exploratory notebook
#
# 
#

import nbformat as nbf
import inspect # To clean up indents
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from setup import start_spark, extract_data
import os

def create_notebook_exploration(df, setup_data = 'setup.py', 
                                output_name = 'test.ipynb',
                                ):
    """
    
    A Function to create a programmatic data exploration notebook

    We have specified a specific kernel spec that matches up with testing env

    current assumptions:
        Dataframes: train, val 

    """

    nb = nbf.v4.new_notebook()

    ############### Checks ##############
    testing = inspect.cleandoc("""
        import os \n
        print(os.getcwd())
    """)

    ############### First Section #######################
    text = inspect.cleandoc("""# Automated Exploration Notebook \n
        This is an automatically generate notebook""")

    import_spark_setup = inspect.cleandoc("""from setup import start_spark, extract_data \n
        sparksesh = start_spark() \n
        train, value = extract_data(sparksesh) \n
        """) 
    
    #.format(setup_data)

    show_train_data = "train.head()"

    ############# cells to explore data #################
    explore_heading = "## View"

    #### Loop through and generate a chart per column
    explore_data = []
    for column in df.columns:
        value = str(column)

        explore_script = inspect.cleandoc("""
        # Using plotly.express
        import plotly.express as px
        
        fig = px.line(train, x=train.index, y='{0}')
        fig.show()
        """.format(value))

        explore_data.append(nbf.v4.new_code_cell(explore_script))


    nb['cells'] = [
            nbf.v4.new_code_cell(testing),
            nbf.v4.new_markdown_cell(text),
            nbf.v4.new_code_cell(import_spark_setup),
            nbf.v4.new_code_cell(show_train_data),
            nbf.v4.new_markdown_cell(explore_heading)]

    nb['cells'].extend(explore_data)

    nb.metadata.kernelspec = {
        "display_name": "Python [conda env:spark]",
        "language": "python",
        "name": "conda-env-spark-py"
    }
    
    nbf.write(nb, output_name)

    return output_name


def run_notebook(output_name):

    """
    execute the notebook

    """
    

    current_dir = os.getcwd()
    
    with open(output_name) as f:
        nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
        ep.preprocess(nb, {'metadata': {'path': current_dir}})

    with open('executed_notebook.ipynb', 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)


if __name__ == '__main__':

    #load_func = 'setup'
    #import setup
    sparksesh = start_spark()
    train, value = extract_data(sparksesh)

    output_file = create_notebook_exploration(df=train) #, 
    
    run_notebook(output_file)
    #setup_data = '{0}.py'.format(load_func))
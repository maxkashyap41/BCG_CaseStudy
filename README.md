# BCG_CaseStudy presented by Madhurjya Das

1. Setting up the virtual environment in the same directory as the Pipfile
    execute command as 
    > make venv 
    
    If you want to reset this:
        unset PIPENV_VENV_IN_PROJECT
    Check if environment variable is set:
        printenv PIPENV_VENV_IN_PROJECT

2. Install Spark Dependencies
    execute command as
    > make build

3. Run the Application to get the Menu Driven Options
    > make run

4. To exit Virtual Environment
    > exit
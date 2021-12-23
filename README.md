# One ML
## OneML is a Fall 2021 Big Data Analytics Coursework project by Anusha, Devica, Aswin from Columbia University's School of Engineering

OneML is primarily a spark stack that supports users' custom ML lifecycle modeling.The Project focuses on building a options to customize an ML lifecycle with different types of sampling, preprocessing, machine learning algorithms etc. OneML serves as a data agnostic platform for varied supervised machine learning requirements. Our platform abstracts the engineering complexity for the ML lifecycle and facilitates rapid provisioning of ML models. Our platform has custom transformations that are not currently offered by Spark MLlib like Stratified Sampling, Stemming, Lemmatization etc.

Developed in Pyspark 3.2.0

## Infrastructure:

**FrontEnd:** 
* FrontEnd is flask application
* Setup:
	* Create a VM instance on GCPpython3 templates/server.py
	* clone the github repo
 	* python3 templates/server.py. This starts the flask application on the VM
   * Get the Publiic URL of the machine from the VM dashboard
   * The application can be accessed from: https:<url>:8111/
     
 **BackEnd:**
 * ```spark-submit main_file.py --configpath config.json```

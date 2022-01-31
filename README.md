# user_search_processor_imo
This Repo contains Documents for User Search analytics Processor for IMO.


**Architectural Diagram**
    
    Architecture Diagram for Coding Excerise_Final.ppt



**user_search_processor_imo.py**


This File Contains the processing Logic for User search Analytics.
One Assumption made is there is already a Data Pipeline For Autosuggest Search Phrases Which is Catalogued.
Using that Already Available Auto Suggest Phrase Catalogue - User Search Data Analytics Processor is written

I have taken AWS Glue 3.0 Approach to Create this Analytics Pipeline.
    
**user_search_processor_imo_stack.yml**
This File Contains CloudFormation Template to deploy this pipeline
1. IAM Roles for Glue Job,Step Functions and EventBridge Rules
2. Glue ETL job
3. Glue Crawlers - To process raw data and final Output.
4. Glue Workflow - To Orchestrate Glue Job and Crawlers
5. Step Function - To Trigger Workflow
6. EvenBridge - To Trigger StepFunctions
           
       
    
    

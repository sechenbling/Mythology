Hello,this is BreeLynn, the following is my graduation project called **Mythology**.  

### INTRODUCTION
&ensp;&ensp;Based on the principle of MLOps, it handles a series of processes in the enterprise from data 
preparation to model training, evaluation and deployment.  

&ensp;&ensp;Through mutual cooperation and internal analysis among multiple modules, users only need
to write a <font color=#90ff66>**yaml file**</font> containing four stages of data, train, evaluation,
and deployment based on MLOps, and **Mythology** will help to manage and execute series of complex 
processes from <font color=#70ffff>**data generation, extract, transform, load and storage to train 
model training, evaluation, and deployment**.</font>

### COMPONENTS

- <font color=#ff6666>Apollo :</font>  
&ensp;&ensp;Responsible for front-end and back-end interaction and MLOps full-process management query and 
exception handling control. MLOps YAML files will be managed, parsed and configured in a unified 
manner then sent to other modules for execution scheduling of specific tasks. Besides, it will 
monitor all exceptions from all modules.


- <font color=cfffff>Diana</font>  
&ensp;&ensp;Responsible for execute specific tasks from Apollo, and it will manage CPU resources and HDFS 
resources on distributed clusters for task environment preparation and resource allocation. And it 
can report real time execute log and task stage to Apollo and inform user.


- <font color=#fddddd>Venus</font>  
&ensp;&ensp;Responsible for front-end ui interaction, guiding users to write MLOps YAML files through a 
simple workflow, providing an interface for pipeline management, configuration changes, and 
exception monitoring.
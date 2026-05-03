This folder contains the Python implementation of the prototype. <br />

The file "json_to_xml.py" contains the main exporter to transform CACAO JSON files to BPMN XML files for visualization and analysis. <br />
The file "json_to_executable_xml.py" contains a restricted approach to create executable BPMN based on "json_to_xml.py". <br />
<br />
The main visualization exporter is used as follows: <br />
The file can be run directly in an IDE such as VS Code. In this case, a file dialog opens and asks for the CACAO JSON input file and the location where the BPMN file should be saved. It can also be run through the terminal by entering: <br />
python json_to_xml.py "your_path/example_file.json" "your_path/example_file.bpmn" <br />
The first argument specifies the JSON file you want to transform and the second argument specifies where the created BPMN file should be stored. <br />
<br />
The exporter for executable BPMN files is used as follows: <br />
It can be run through the terminal by entering: <br />
python json_to_executable_xml.py "your_path/example_file.json" "your_path/example_file.bpmn" --task-mode script --decision-step-name "example_step" --decision-default true <br />
Again, the first argument specifies the playbook and the second argument specifies where the BPMN file should be saved. The option "--task-mode script" creates script tasks. The option "--decision-step-name" specifies the step that sets the decision variable, and "--decision-default" defines its default value.

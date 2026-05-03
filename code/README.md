This folder contains the Python implementation of the prototype. <br />

The file "json_to_xml.py" contains the main exporter to transform CACAO JSON files to BPMN XML files for visualization and analysis.  <br />
The file "json_to_executable_xml.py" contains a restricted approach to create executable BPMN based on "json_to_xml.py". <br />
<br />
The main visualization exporter is used as follows: <br />
The file can be run directly in your IDE. Also the file can be run through the terminal by entering <br />
python json_to_xml.py "your_path/example_file.json" "your_path/example_file.bpmn" <br />
The first argument specifies the JSON file you want to transform and the second argument makes you choose where to store the created BPMN file. <br />
<br />
Usage example for the restricted executable proof of concept: <br />
python json_to_executable_xml.py "your_path/example_file.json" "your_path/example_file.bpmn" --task-mode script --decision-step-name "example_step" --decision-default true <br/>
Again the first argument asks for a playbook, the second argument asks for a location to save the BPMN file. The tasks are designed to be script tasks and the step following a decision can be chosen.

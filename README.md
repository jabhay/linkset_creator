# linkset_creator
Python code for building a linkset using web and local resources.

## Algorithm

currently only performs joins using point in polygon joining functions.

The link_creator users the following algorithm:
1. Get a list of ids to process
2. Loop through the ids in the list (multithreaded)
2.1 Get the point
2.2 Perform the point in polygon to obtain the ID from a reference dataset
2.3 Write the result to the file

## Output File Structure

The output file is a comma separated value (CSV) file, containing the following fields:
1. An identifier for the established link
2. An identifier for the item used to drive the joins
3. An identifier for the corresponding item from the reference dataset

## Configuration options

In the options below, the following are indicated in brackets:

- Polygon: indicates a parameter used for the Polygon model
- WFS: indicates a parameter used by the WFS specialisation of the Polygon model
- Model: indicates a parameter used by the Point model

### endpoint (Polygon)
The endpoint of the polygon 

### geom (Polygon)
The name of the polygon geometry attribute

### layer (Polygon, WFS)
The name of the WFS layer to query

### layerid (Polygon, WFS) 
The name of the identifier attribute of the WFS layer

### nsshort (Polygon, WFS)
This is the namespace prefix for the WFS layer identifier, used to parse the XML returned

### nsurl (Polygon, WFS)
This is the namespace URL for the WFS layer identifier, used to parse the XML returned

### function (Polygon, WFS)
The name of the WFS filter to apply (e.g., Contains, Intersects)

### register_model (Model)
The model to use to drive linking. Choose from:
1. DBModel: Use a database to drive the process
2. LDAPIModel: Use the PyLDAPI Python Linked Data API to drive the process

### register_endpoint (Model)
The endpoint of the model to use to drive linking. When this is set to LDAPIModel, this is the endpoint of the register. When the model is set to DBModel, this will be the PostgreSQL connection string in the form: <br>
dbname='<db_name>' user='<db_user>' host='<db_host>' password='<db_password>'.

### start (Model)
The batch page to start from

### stop (Model)
The number of batches to run before stopping

### batch_size (Model)
The number of records to process as a batch each time

### output_file
The file to write the results to

### threads
The number of threads to process simultaneously

### batch_id
A starting sequence to use for the identifier for each established link

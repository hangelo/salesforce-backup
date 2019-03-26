<?php

error_reporting( E_ALL );
ini_set('display_errors', 1);

// Salesforce API
require 'vendor/autoload.php';


/********************************************************************************************************************************
Auxiliare functions
********************************************************************************************************************************/

function MyEcho($msg)
{
    /**
    Echo something to the client without buffering

    :param $msg String:
        The string we want to Echo
    */
    $msg = str_replace('<br>', "\n", $msg);

    echo '(Memory usage:'.memory_get_usage(true).' / Memory peak:'.memory_get_peak_usage(true).') '.$msg."\n";
    //ob_flush();
    //flush();
}

function MyEchoEnterFunction($name_of_the_function)
{
    echo "\n\n";
    echo '-----------------------------------------------------------------------------------';
    echo "\n";
    MyEcho('Entering on function "'.$name_of_the_function.'"');
    echo "\n";
}


/********************************************************************************************************************************
Auxiliare functions
********************************************************************************************************************************/

class ReadFromSalesforce {

    // Salesforce credentials
    private $SALESFORCE_URL = 'https://login.salesforce.com';
    private $SALESFORCE_VERSION = '32.0';
    private $CONSUMER_KEY = '';
    private $CONSUMER_SECRET = '';
    private $SALESFORCE_LOGIN = '';
    private $SALESFORCE_PASSWORD = '';
    private $SALESFORCE_TOKEN = '';

    // These are the tables we query contents
    private $OBJECTS_TO_COLLECT = array(
        'Account',
        'AccountContactRole',
        'Approval',
        'Campaign',
        'CampaignMember',
        'Case',
        'CaseComment',
        'CaseContactRole',
        'CaseHistory2',
        'CaseSolution',
        'CollaborationGroupRank',
        'Contact',
        'Lead',
        'Opportunity',
        'OpportunityLineItem',
        'Quote',
        'QuoteLineItem'
    );



    // Type of fields on Salesforce
    private $FIELD_TYPES = [
        'address' => 'varchar(255)',
        'base64' => 'text',
        'boolean' => 'boolean',
        'combobox' => 'varchar(255)',
        'currency' => 'float(20,8)',
        'date' => 'varchar(50)',
        'datetime' => 'varchar(50)',
        'double' => 'float(20,8)',
        'email' => 'varchar(255)',
        'encryptedstring' => 'varchar({length})',
        'id' => 'varchar(20)',
        'int' => 'int',
        'multipicklist' => 'text',
        'percent' => 'float(20,8)',
        'phone' => 'varchar(255)',
        'picklist' => 'varchar(255)',
        'reference' => 'varchar(255)',
        'string' => 'varchar({length})',
        'textarea' => 'text',
        'url' => 'varchar(255)',
    ];


    // The directory that we will save all temporary files
    private $WORK_DIRECTORY = '/home/company/www/intranet/read-from-salesforce/post/';

    // File name to store the return of getAllObjects API function
    private $FILENAME_GETALLOBJECTS = '/home/company/www/intranet/read-from-salesforce/post/_getAllObjects';

    // File name to store the content of "Objects" variable
    private $FILENAME_OBJECTS = '/home/company/www/intranet/read-from-salesforce/post/_ArrayObjects';

    // File name to store information of the Fields of an Object
    // THis name is joined to the self name of the Object
    private $FILENAME_OBJECTFIELDS = '/home/company/www/intranet/read-from-salesforce/post/_object_fields_';

    /**
    Array of all objects

    'name' =>
        String
        Name of the object
    'is_custom' =>
        Boolean
        If tthe object is a custom or standard object
    'fields' =>
        Array
        All fields of each object
    */
    public $objects = [];


    // The Salesforce Connection
    private $salesforce;


    public function __construct()
    {
        $this->salesforce = new SalesforceRestAPI\SalesforceAPI($this->SALESFORCE_URL, $this->SALESFORCE_VERSION, $this->CONSUMER_KEY, $this->CONSUMER_SECRET);
        $this->salesforce->login($this->SALESFORCE_LOGIN, $this->SALESFORCE_PASSWORD, $this->SALESFORCE_TOKEN);
    }


    public function ConvertSFTypeToDBType($type, $length)
    {
        /**
        Convert the Type of a field in Salesforce to a MySQL type field
        */

        try {
            if (($type == 'string' || $type == 'encryptedstring') && (int)$length > 1000) {
                return 'text';
            }
            $r = str_replace('{length}', $length, $this->FIELD_TYPES[$type]);
        } catch (Exception $e) {
            print_r($type);
            echo "\n";
            print_r($length);
            echo "\n";
            print_r($e);
            exit;
        }

        return $r;
    }


    public function GetAllObjects()
    {
        /**
        Get all information from known objects.
        The informations includes:
            - The name of the object
            - If it is a custom object
        */

        // Put a separator on debug information at the start of this function
        MyEchoEnterFunction(__FUNCTION__);

        // Get all objects
        $attempts = 0;
        $success = false;
        do {
            try {
                $all_objects = $this->salesforce->getAllObjects();
                $success = true;
            } catch (Exception $e) {
                $attempts++;
                MyEcho('ERROR ON "getQueryFromUrl" -> attempts: '.$attempts);
                sleep(5); // 5 seconds delay
            }
        } while (!$success && $attempts < 5);

        //Put result on a file
        file_put_contents($this->FILENAME_GETALLOBJECTS, json_encode($all_objects));

        // Get all objects
        $qt_all_objects = count($all_objects['sobjects']);
        for ($i = 0; $i < $qt_all_objects; $i++) {
            $object_name = $all_objects['sobjects'][$i]['name'];
            $object_is_custom = $all_objects['sobjects'][$i]['custom'];

            $object_is_custom = $object_is_custom == 1 ? true : false;

            // There are some objects that we will ignore because they can't accept Query commands
            if (!in_array($object_name, $this->OBJECTS_TO_COLLECT)) {
                MyEcho('Ignore Object: '.$object_name);
                continue;
            }

            MyEcho('Will query Object: '.$object_name);
            $this->objects[$object_name] = array(
                'name' => $object_name,
                'is_custom' => $object_is_custom,
                'fields' => null
            );
        }

        file_put_contents($this->FILENAME_OBJECTS, json_encode($this->objects));
    }


    private function GetFields($object_name)
    {
        /**
        Get all the fields from an Object on Salesforce
        The information we collect from each field:
            - The name of the field
            - The type of the field
            - The length of the field

        :param $object_name String
            The API name of the Object on Salesforce
        */

        // Put a separator on debug information at the start of this function
        MyEchoEnterFunction(__FUNCTION__);

        // Table name to get all fields
        $this->objects[$object_name]['fields'] = Array();
        MyEcho('Collecting fields from Object: '.$object_name);

        // Get all fields of the object
        // If fail, we try up to five times
        $attempts = 0;
        $success = false;
        do {
            try {
                $the_object = $this->salesforce->getObjectMetadata($object_name, true);
                $success = true;
            } catch (Exception $e) {
                $attempts++;
                MyEcho('ERROR ON "getQueryFromUrl" -> attempts: '.$attempts);
                sleep(5); // 5 seconds delay
            }
        } while (!$success && $attempts < 5);

        // Array of fields to be returned by this function
        $array_of_fields = [];

        // Get all fields from the object
        $qt_fields = count($the_object['fields']);
        for ($i = 0; $i < $qt_fields; $i++) {
            $field_name = $the_object['fields'][$i]['name'];
            $field_type = $the_object['fields'][$i]['type'];
            $field_length = $the_object['fields'][$i]['length'];

            $array_of_fields[$field_name] = array(
                'name' => $field_name,
                'type' => $field_type,
                'length' => $field_length
            );
        }

        // Return all field names
        return $array_of_fields;
    }


    public function GetAllFields()
    {
        /**
        Get all Fields from all Objects on Salesforce
        */

        // Put a separator on debug information at the start of this function
        MyEchoEnterFunction(__FUNCTION__);

        // Get all Fields from all Objects
        foreach ($this->objects as $object_name => $object) {
            $this->objects[$object_name]['fields'] = self::GetFields($object_name);
            file_put_contents($this->FILENAME_OBJECTFIELDS.$object_name, json_encode($this->objects[$object_name]['fields']));
        }
    }


    private function WriteRecordsIntoFile(&$records, $file_name, &$fields, $object_name, &$already_writed, $total_items, $write_to_database, $wrte_to_csv_files)
    {
        /**
        Write to file all records from an Object

        :param &$records:
            Passed by reference
            The items to write on the file

        :param $file_name:
            The name of the file

        :param &$fields:
            Passed by reference
            The fields we will read from the Records

        :param $object_name:
            The name of the Object we are writing

        :param &$already_writed:
            Passed by reference
            Number of items we already write on the file

        :param $total_items
            Total number of items we need to query from the Salesforce

        :param $write_to_database Boolean:
            TRUE if we want to write to the local Database

        :param $wrte_to_csv_files Boolean:
            TRUE if we want to write to CSV files
        */

        $records_to_db = '';

        foreach ($records as $record) {

            $csv_line = '';
            $db_line = '';

            foreach ($fields as $field_name => $field) {
                if ($field['type'] == 'address') {
                    continue;
                }

                $value_to_csv = $record[$field_name];
                $value_to_csv = str_replace('\\', '\\\\', $value_to_csv);
                $value_to_csv = str_replace('\'', '\\\'', $value_to_csv);
                $value_to_csv = str_replace('"', '\"', $value_to_csv);

                $csv_line .= ($csv_line != '' ? ',' : '').'"'.$value_to_csv.'"';

                $value_to_db = 'null';
                switch ($field['type']) {
                    case 'address' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'base64' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'boolean' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'combobox' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'currency' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'date' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'datetime' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'double' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'email' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'encryptedstring' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'id' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'int' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'multipicklist' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'percent' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'phone' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'picklist' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'reference' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'string' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'textarea' :
                        $value_to_db = $record[$field_name];
                        break;
                    case 'url' :
                        $value_to_db = $record[$field_name];
                        break;
                    default :
                        print_r($field);
                        MyEcho('### COULD NOT DETERMINE THE FIELD TYPE: '.$field['type']);
                        exit;
                }

                $_value_to_db = $value_to_db;
                //$value_to_db = preg_replace(array('/\xF4/', '/\x80/', '/\x13/', '/\xE2/', '/\x8B/', '/\x88/', '/\x92/', '/\x93/', '/\xC2/', '/\xBA/'), '', $value_to_db);
                $value_to_db = str_replace('–', '-', $value_to_db);
                $value_to_db = str_replace('•', '-', $value_to_db);
                $value_to_db = iconv("UTF-8","ISO-8859-1//IGNORE",$value_to_db);
                $value_to_db = iconv("ISO-8859-1","UTF-8",$value_to_db);

                $value_to_db = str_replace('\\', '\\\\', $value_to_db);
                $value_to_db = str_replace('\'', '\\\'', $value_to_db);
                $value_to_db = str_replace('"', '\"', $value_to_db);

                if ($value_to_db == '') $value_to_db = 'null';
                else $value_to_db = '"'.$value_to_db.'"';
                $_value_to_db = $value_to_db;
                $db_line .= ($db_line != '' ? ',' : '').$value_to_db;
            }


            // Write the values to the CSV file
            if ($wrte_to_csv_files) {
                $csv_content = $csv_line."\n";
                file_put_contents($this->WORK_DIRECTORY.$file_name, $csv_content, FILE_APPEND);
            }


            // Write the values to the database
            if ($write_to_database) {

                $records_to_db = ('('.$db_line.')');
                // We only insert to the database if we have at least one record
                if ($records_to_db) {

                    // Fields to query from Salesforce
                    $fields_to_query = '';
                    foreach ($fields as $field_name => $field) {
                        if ($field['type'] == 'address') {
                            continue;
                        }
                        $fields_to_query .= ($fields_to_query != '' ? ',' : '').$field_name;
                    }

                    try {
                        // Get an active instance and start a transaction
                        StartTransaction($conn, $transaction);

                        $sql = self::SQLToInsertIntoTable($object_name, $fields_to_query, $records_to_db);
                        file_put_contents($this->WORK_DIRECTORY.'_SQL_'.$object_name.'.sql', $sql."\n", FILE_APPEND);
                        $qry = $conn->prepare($sql);
                        $qry->execute();

                        // Commit the transaction and close the database connection
                        CommitTransaction($conn, $transaction);
                    } catch (Exception $e) {
                        // Performe a Rollback function, close the database connection and raise an error
                        RollbackTransaction($conn, $transaction, $e->getMessage());
                    }
                }
            }
        }



        $already_writed += count($records);
        MyEcho('Object: '.$object_name.' / Records writing: '.(count($records)).' / already writed: '.$already_writed.'/'.$total_items);
    }


    private function DoQueryFromSalesforce($fields, $object)
    {
        /**
        Make a query on Salesforce. If fail, try again up to 5 times

        :param $fields String:
            Fields we want to query, comma separated

        :param $object String:
            The name of the object

        :return JSON:
            The result of the query
        */

        $MAXIMUM_RETRIES = 5;
        $SLEEP_BETWEEN_RETRIES = 5; // seconds

        $attempts = 0;
        $success = false;
        $response = null;
        do {
            try {
                $response = $this->salesforce->searchSOQL('SELECT '.$fields.' FROM '.$object, true);
                $success = true;
            } catch (Exception $e) {
                $attempts++;
                MyEcho('ERROR ON "searchSOQL" -> attempts: '.$attempts);
                sleep($SLEEP_BETWEEN_RETRIES);
            }
        } while (!$success && $attempts < $MAXIMUM_RETRIES);

        if ($response == null) {
            MyEcho('FAIL TO CONNECT TO SALESFORCE AFTER '.$MAXIMUM_RETRIES.' TRIES');
            exit;
        }

        return $response;
    }


    private function WriteObjectIntoDatabaseAndCSVFile($object_name, $write_to_database, $wrte_to_csv_files)
    {
        /**
        Make a query on Salesforce using an SOQL statement and write the records into a file

        :param $write_to_database Boolean:
            TRUE if we want to write to the local Database

        :param $wrte_to_csv_files Boolean:
            TRUE if we want to write to CSV files
        */

        MyEcho('Get all records from '.$object_name);

        // Fields to query from Salesforce
        $fields_to_query = '';
        foreach ($this->objects[$object_name]['fields'] as $field_name => $field) {
            $fields_to_query .= ($fields_to_query != '' ? ',' : '').$field_name;
        }

        // Create a CSV file for each Object
        $file_name = $object_name.'.csv';
        $csv_line = '';
        foreach ($this->objects[$object_name]['fields'] as $field_name => $field) {
            $csv_line .= ($csv_line != '' ? ',' : '').'"'.$field_name.'"';
        }
        $csv_content = $csv_line."\n";
        file_put_contents($this->WORK_DIRECTORY.$file_name, $csv_content, FILE_APPEND);

        // Get records from the current Object
        $response = self::DoQueryFromSalesforce($fields_to_query, $object_name);
        $records = $response['records'];
        $total_items = $response['totalSize'];
        $already_writed = 0;

        // Get records and put into file
        self::WriteRecordsIntoFile($records, $file_name, $this->objects[$object_name]['fields'], $object_name, $already_writed, $total_items, $write_to_database, $wrte_to_csv_files);

        if (array_key_exists('nextRecordsUrl', $response)) {
            do {
                $nextRecordsUrl = $response['nextRecordsUrl'];
                unset($response);
                $response = null;

                // If fail, we try up to five times
                $attempts = 0;
                $success = false;
                do {
                    try {
                        $response = $this->salesforce->getQueryFromUrl($nextRecordsUrl);
                        $success = true;
                    } catch (Exception $e) {
                        $attempts++;
                        MyEcho('ERROR ON "getQueryFromUrl" -> attempts: '.$attempts);
                        sleep(5); // 5 seconds delay
                    }
                } while (!$success && $attempts < 5);
                $records = $response['records'];

                // Get records and put into file
                self::WriteRecordsIntoFile($records, $file_name, $this->objects[$object_name]['fields'], $object_name, $already_writed, $total_items, $write_to_database, $wrte_to_csv_files);

            } while (array_key_exists('nextRecordsUrl', $response));
        }

        unset($response);
        $response = null;

        unset($records);
        $records = null;
    }


    function WriteAllObjectsIntoDatabaseAndCSVFiles($write_to_database, $wrte_to_csv_files)
    {
        /**
        For each Object, make a query on Salesforce using an SOQL statement and write the records into a file

        :param $write_to_database Boolean:
            TRUE if we want to write to the local Database

        :param $wrte_to_csv_files Boolean:
            TRUE if we want to write to CSV files
        */

        // Put a separator on debug information at the start of this function
        MyEchoEnterFunction(__FUNCTION__);

        foreach ($this->objects as $object_name => $object) {
            self::WriteObjectIntoDatabaseAndCSVFile($object_name, $write_to_database, $wrte_to_csv_files);
        }
    }


    function SQLToCreateTableOnDatabase($table_name)
    {
        $fields = '';
        foreach ($this->objects[$table_name]['fields'] as $field_name => $field) {
            $fields .= $field_name.' '.(self::ConvertSFTypeToDBType($field['type'], $field['length'])).',';
        }

        $sql = '
            CREATE TABLE sf_'.$table_name.' (
                ss_id Int NOT NULL AUTO_INCREMENT,
                imp_prc_id int COMMENT \'Foreign key to the import_process table\',
                ss_datetime_added timestamp COMMENT \'When the record was added to this table\',
                ss_datetime_modified timestamp COMMENT \'When the record was modified on this table\',
                '.$fields.'
                PRIMARY KEY (ss_id),
                INDEX i_'.$table_name.'_id (id)
            )
            ENGINE=InnoDB
            AUTO_INCREMENT=1
            COLLATE=utf8_general_ci
        ;';
        return $sql;
    }


    function SQLToDropTableOnDatabase($table_name)
    {
        /**
        Return the command to Drop a table on database

        :param $table_name String:
            The name of the table

        :return String:
            SQL Command
        */
        $sql = 'DROP TABLE IF EXISTS sf_'.$table_name.';';
        return $sql;
    }

    function SQLToInsertIntoTable($table_name, $fields, $values)
    {
        /**
        Return the command to Insert Items into a table on database

        :param $table_name String:
            The name of the table

        :param fields String:
            The fields separated by comma

        :param values String:
            The values separated by comma

        :return String:
            SQL Command
        */
        $sql = 'INSERT INTO sf_'.$table_name.' ('.$fields.') VALUES '.$values.';';
        return $sql;
    }


    function RemoveFilesOnWorkDirectory()
    {
        /**
        Remove all files on Working Directory
        */
        $dir = $this->WORK_DIRECTORY;
        $di = new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS);
        $ri = new RecursiveIteratorIterator($di, RecursiveIteratorIterator::CHILD_FIRST);
        foreach ( $ri as $file ) {
            $file->isDir() ?  rmdir($file) : unlink($file);
        }
        return true;
    }
}




$process = new ReadFromSalesforce();

// Remove old files
$process->RemoveFilesOnWorkDirectory();

// Get all objects from Salesforce
$process->GetAllObjects();

// Get all field informations
$process->GetAllFields();

// Write all values from Salesforce to local Database and/or CSV files
$process->WriteAllObjectsIntoDatabaseAndCSVFiles(false, true);

MyEcho('END');











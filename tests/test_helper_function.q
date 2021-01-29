// test_helper_function.q

// Open namespace test
\d .test

// --------------- TEST GLOBALS --------------- //

// Define enum representing status of executing a function
EXECUTION_STATUS__:`Ok`Error;
EXECUTION_ERROR__:`.test.EXECUTION_STATUS__$`Error;
EXECUTION_OK__:`.test.EXECUTION_STATUS__$`Ok;

/
* @brief Check if execution fails and teh returned error matches a specified message
* @param func: interface function to apply
* @param args: list of arguments to pass to the function
* @param errkind: string error kind message to expect. ex.) "Invalid scalar type"
* @return boolean
\
ASSERT_ERROR:{[func; args; errkind]
  res:.[func; args; {[err] (EXECUTION_ERROR__; err)}];
  $[EXECUTION_ERROR__ ~ first res; 
    res[1] like errkind,"*";
    0b
  ]
 }

/
* @brief Check if comparison succeeds
* @param func: interface function to apply
* @param args: list of arguments to pass to the function
* @param target: target to compare
* @return boolean
\
ASSERT_TRUE:{[func; args; target]
  .[func; args] ~ target
 }

/
* @brief Check if comparison succeeds
* @param func: interface function to apply
* @param args: list of arguments to pass to the function
* @param target: target to compare
* @return boolean
\
ASSERT_EQUAL_AT:{[func; args; target; arg_num]
  (.[func; args])[arg_num] ~ target
 }

/
* @brief Check if comparison fails
* @param func: interface function to apply
* @param args: list of arguments to pass to the function
* @param target: target to compare
* @return boolean
\
ASSERT_FALSE:{[func; args; target]
  not ASSERT_TRUE[func; args; target]
 }

// ------------------- END -------------------- //

// Close namespace
\d .
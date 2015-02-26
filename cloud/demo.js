// Enter username and password here
var username="root";
var password="NTTdocomo";

// URI for the master - if the script is hosted locally, this should be e.g.
// 'http://192.168.0.1/'
var master_uri="http://192.168.0.173/";

// Are we using the json values or not?
var use_json=true;

// This is where we stick our xenapi session
var mysession;
var vmConsoleApplets = new Array();


$(document).ready(function(){
// If the script isn't being served from the XenServer, firefox requires you to
// obtain privileges to access data across domains, so we need to uncomment the
// following line:
//   netscape.security.PrivilegeManager.enablePrivilege("UniversalBrowserRead");

  // For the special /json API handler, we get our results back as a json encoded
  // string, so we eval them to obtain the actual data we want
  function maybe_eval(result) {
    if(use_json) {
      return eval("("+result+")");
    } else {
      return result;
    }
  }

  function appendOption(key , val){
	var elOptNew = document.createElement('option');
	elOptNew.text = key;
	elOptNew.value = val;
	var elSel = document.getElementById('vmSelection');
	try {
		elSel.add(elOptNew, null); // standards compliant; doesn't work in IE
	}
		catch(ex) {
		elSel.add(elOptNew); // IE only
	}
  }
  
  function demo() {
  // As above...
    netscape.security.PrivilegeManager.enablePrivilege("UniversalBrowserRead");

    var session_result = rpc.session.login_with_password(username,password);
    if(session_result.Status=="Failure") {
      alert("Failed to log in!");
      raise("Failed to log in");
    }

    mysession = maybe_eval(session_result.result.Value);
	
    var vms_result = rpc.VM.get_all_records(mysession);
    var vms = maybe_eval(vms_result.result.Value);

    // Get all records calls return a map of ref -> record. Iterate through the refs
    // and fill in our div with the name_label field of the record
	
	var counter = 0;
	
	for(vm in vms) {
	  if(vms[vm].is_a_template == 1 || vms[vm].power_state != 'Running') continue;
		
		var vm_consoles_result = rpc.VM.get_consoles(mysession, vm);
		var vm_consoles = maybe_eval(vm_consoles_result.result.Value);		
		var console_result = rpc.console.get_record(mysession, vm_consoles[0]); 
		var console = maybe_eval(console_result.result.Value);						
		
		var vmHTML  = "<applet archive='XenServerConsole.jar' code=com.citrix.xenserver.console.Initialize.class width='1000' height='800'>" +					
				       "<param name=SESSION value='" + mysession + "'>" + 
				       "<param name=URL value='" + console.location +  "'>" + 
				       "<PARAM NAME=USEURL VALUE='true'></applet>";
		
		vmConsoleApplets[counter] = vmHTML;
		
		appendOption(vms[vm].name_label, counter);
		//$('#vms').append("<p>"+vms[vm].name_label+"</p>");
		//$('#vms').append("<p>"+console.location+"</p>");		
		counter ++;
		
    }
  }

  // Here we actully create the rpc object, explicitly listing the API calls we'll
  // be making. Newer versions of XenServer support the system.listMethods call
  // so we don't need to do this
  var actual_uri = master_uri + (use_json ? "json" : "");

  rpc = new $.rpc(
    actual_uri,
    "xml",
    demo,
    null,
    ["session.login_with_password","VM.get_all_records","VM.get_consoles", "console.get_record"]
  );
});

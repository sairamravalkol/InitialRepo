
/* ==================			 Time related functions 			================== */

function getTimeOffset()
{
var offsetInMicroseconds = 10800000000;
return offsetInMicroseconds;
}

function getStartTime()
{
// get start time as string
var start = document.getElementById("start_date").value + "T" + document.getElementById("start_time").value;
// convert start to date with time
var startDateWithTime = new Date(start);
var startDateWithTimeInEpoch = startDateWithTime.getTime(); 
// add microseconds from hidden field - populated during getDuration API
var startTimeMicroseconds = document.getElementById("start_time_microseconds").value;
	if (startTimeMicroseconds=='' || Number(startTimeMicroseconds)==0)
	{
	startTimeMicroseconds='000';	
	}

var startDateWithTimeInEpochWithMicrosec =Number(startDateWithTimeInEpoch +''+ startTimeMicroseconds);
return startDateWithTimeInEpochWithMicrosec; 
}

function getEndTime()
{
// get end time as string
var end = document.getElementById("end_date").value + "T" + document.getElementById("end_time").value;
// convert end to date with time	
var endDateWithTime = new Date(end);
var endDateWithTimeInEpoch = endDateWithTime.getTime(); 
// add microseconds from hidden field - populated during getDuration API
var endTimeMicroseconds = document.getElementById("end_time_microseconds").value;
	if (endTimeMicroseconds=='' || Number(endTimeMicroseconds)==0)
	{
	endTimeMicroseconds='000';	
	}
var endDateWithTimeInEpochhWithMicrosec =Number(endDateWithTimeInEpoch +''+ endTimeMicroseconds);
return endDateWithTimeInEpochhWithMicrosec; 
}

function setStartTimeField(start_time) {
	
//	var additionStart =start_time+''; 
	var additionStart = Number(start_time) - getTimeOffset();
	additionStart = additionStart+'';
	
	var startTimeWithoutMicroSeconds = Number(additionStart.substring(0,13));
	var microSecondPartOfStartTime = Number(additionStart.substring(13,additionStart.length));	
	start_time = startTimeWithoutMicroSeconds;	
	document.getElementById("start_time_microseconds").value=microSecondPartOfStartTime;

    var start_readable_date = new Date(start_time);
    var range_start_date = date_format(start_readable_date);
    var range_start_time = time_format(start_readable_date);	
	
    document.getElementById("start_date").defaultValue = range_start_date;
    document.getElementById("start_time").defaultValue = range_start_time;
    document.getElementById("start_date").value = range_start_date;
    document.getElementById("start_time").value = range_start_time;
			
}

function setEndTimeField(end_time) {

	// var additionEnd = end_time+''; 
	var additionEnd = Number(end_time) - getTimeOffset();
	additionEnd = additionEnd+'';
	
	var endTimeWithoutMicroSeconds = Number(additionEnd.substring(0,13));
	var microSecondPartOfEndTime = Number(additionEnd.substring(13,additionEnd.length));
	end_time = endTimeWithoutMicroSeconds;
	document.getElementById("end_time_microseconds").value=microSecondPartOfEndTime;

    var end_readable_date = new Date(end_time);
    var range_end_date = date_format(end_readable_date);
	var range_end_time = time_format(end_readable_date);
	
    document.getElementById("end_date").defaultValue = range_end_date;
    document.getElementById("end_time").defaultValue = range_end_time;
    document.getElementById("end_date").value = range_end_date;
    document.getElementById("end_time").value = range_end_time;	
	
		
}

function format_two_digits(n) {
    return n < 10 ? '0' + n : n;
}

function time_format(d) {
    hours = format_two_digits(d.getHours());
    minutes = format_two_digits(d.getMinutes());
    seconds = format_two_digits(d.getSeconds());
    milliseconds = format_two_digits(d.getMilliseconds());
    return hours + ":" + minutes + ":" + seconds + "." + milliseconds;
}

function date_format(d) {
    if (d !== undefined && d !== null) {
        var yyyy = d.getFullYear().toString();
        var mm = (d.getMonth() + 1).toString(); // getMonth() is zero-based
        var dd = d.getDate().toString();

        if (mm.length < 2) // we need the date it double digit format. i.e June = 06 and not 6
        {
            mm = "0" + mm;
        }

        if (dd.length < 2) // we need the day in double digit number. for example the fifth = 05 and not 5
        {
            dd = "0" + dd;
        }
        var formatted_date = yyyy + "-" + mm + "-" + dd;
        return formatted_date;
    }
    else {
        console.log("time object is not defined.");
    }
}
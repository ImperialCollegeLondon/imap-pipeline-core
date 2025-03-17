function emptyCalibrator(date, sciencefile, calfile, datastore, configfile)

day_to_process = datetime(date);

baseScience = readstruct(sciencefile);

epoch = [baseScience.values.time];
values = {baseScience.values.value};

offset_values = zeros(length(epoch), 3);

Metadata.dependencies = [];
Metadata.science = [sciencefile];
Metadata.creation_timestamp = datetime("now");
Metadata.comment = "This is a zero value calibration file";
generateCalFile(epoch, offset_values, "noop", Metadata, calfile)

end

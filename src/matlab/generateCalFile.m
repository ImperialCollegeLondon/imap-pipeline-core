function generateCalFile(epoch, values, timedeltas, quality_flags, quality_bitmasks, method, metadata, outputFile)

Calibration.id = "";
Calibration.mission = "IMAP";

Validity.start = epoch(1);
Validity.end = epoch(end);
Calibration.validity = Validity;

Calibration.method = method;
Calibration.sensor = "MAGo";
Calibration.version = 0;

Metadata = metadata;
Calibration.metadata = Metadata;

Calibration.value_type = "vector";

 for i=length(epoch):-1:1
    % Force time to show maximum second specificity without rounding errors
     Value.time = string(epoch(i), 'yyyy-MM-dd''T''HH:mm') + ":" + num2str(epoch(i).Second, "%09.6f");
     Value.value = values(i,:);
     Value.timedelta = timedeltas(i);
     Value.quality_flag = quality_flags(i);
     Value.quality_bitmask = quality_bitmasks(i);

     Calibration.values(i)=Value;
 end

writestruct(Calibration, outputFile)

end

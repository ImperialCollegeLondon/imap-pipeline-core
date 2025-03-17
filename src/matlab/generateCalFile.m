function generateCalFile(epoch, values, method, metadata, outputFile)

Calibration.id = "";
Calibration.mission = "IMAP";

Validity.start = epoch(1);
Validity.end = epoch(end);
Calibration.validity = Validity;

Calibration.method = method;
Calibration.sensor = "MAGo";

Metadata = metadata;
Calibration.metadata = Metadata;

Calibration.value_type = "vector";

 for i=length(epoch):-1:1
     Value.time = epoch(i);
     Value.value = values(i,:);
     Value.timedelta = 0;
     Calibration.values(i)=Value;
 end

writestruct(Calibration, outputFile)

end

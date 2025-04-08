function emptyCalibrator(date, sciencefile, calfile, datastore, config)
    arguments
        date string
        sciencefile string
        calfile string
        datastore string
        config string
    end

day_to_process = datetime(date);

baseScience = readstruct(sciencefile);

epoch = [baseScience.values.time];
values = {baseScience.values.value};

offset_values = zeros(length(epoch), 3);
timedelta = zeros(length(epoch), 1);
quality_flags = zeros(length(epoch), 1);
quality_bitmask = zeros(length(epoch), 1);

num_vals = length(values);

for i = 1:20
    rand_negatives = truncate(-100 * rand([20 1]),6);
    rand_positives = truncate(100 * rand([20 1]),6);
    offset_values(1:20,1) = rand_negatives;
    offset_values(20:39,1) = rand_positives;
end

for i = 1:20
    rand_negatives = truncate(-100 * rand([20 1]),6);
    rand_positives = truncate(100 * rand([20 1]),6);
    offset_values(40:59,2) = rand_negatives;
    offset_values(60:79,2) = rand_positives;
end

for i = 1:20
    rand_negatives = truncate(-100 * rand([20,1]),6);
    rand_positives = truncate(100 * rand([20,1]), 6);
    offset_values(80:99,3) = rand_negatives;
    offset_values(100:119,3) = rand_positives;
end

offset_values(120:139,:) = NaN([20 3]);

for i = 1:20
    rand_nums = 200*rand([20,3]) - 100;
    offset_values(140:159,:) = truncate(rand_nums,6);
end

timedelta(160:179) = truncate(-1 * rand([20 1]),3);
timedelta(180:199) = truncate(rand([20 1]),3);

quality_flags(200:209) = 1;

quality_flags(210:219) = round(rand([10 1]));

for i= 1:7
quality_bitmask(220+(i-1)*5:220+i*5-1) = bitshift(1,i);
end

quality_bitmask(255) = 11;
quality_bitmask(256) = 12;
quality_bitmask(257) = 63;
quality_bitmask(258) = 27;
quality_bitmask(259) = 109;

Metadata.dependencies = [];
Metadata.science = {sciencefile};
Metadata.creation_timestamp = string(datetime("now"), 'yyyy-MM-dd''T''HH:mm:ss');
Metadata.comment = "This is a zero value calibration file";
generateCalFile(epoch, offset_values, timedelta, quality_flags, quality_bitmask, "noop", Metadata, calfile)

end

function n= truncate(num,digits)
n=fix(num*10^digits)/10^digits;
end

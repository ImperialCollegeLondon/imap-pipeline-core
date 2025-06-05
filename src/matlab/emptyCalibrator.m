function emptyCalibrator(date, sciencefile, calfile, datastore, config)
    arguments
        date string
        sciencefile string
        calfile string
        datastore string
        config string
    end

day_to_process = datetime(date);
day_before = day_to_process - days(1);
day_after = day_to_process + days(1);

disp("Loading science file: " + sciencefile);
baseScience = readstruct(sciencefile);

disp("Generating offsets for " + datestr(day_to_process, 'yyyy-mm-dd'));
epoch = [baseScience.values.time];
values = {baseScience.values.value};

all_in_day_idx = epoch > dateshift(day_before, 'end', 'day') & epoch < dateshift(day_after, 'start', 'day');

offset_values = zeros(length(epoch), 3);
timedelta = zeros(length(epoch), 1);
quality_flags = zeros(length(epoch), 1);
quality_bitmask = zeros(length(epoch), 1);

% subset_offset_values = offset_values(all_in_day_idx, :);
% subset_timedelta = timedelta(all_in_day_idx);
% subset_quality_flags = quality_flags(all_in_day_idx);
% subset_quality_bitmask = quality_bitmask(all_in_day_idx);
% subset_epoch = epoch(all_in_day_idx);

% num_vals = length(subset_epoch);
% chunk_size = floor(num_vals/15);

% % Generate random x values
% rand_negatives = truncate(-100 * rand([chunk_size 1]),6);
% rand_positives = truncate(100 * rand([chunk_size 1]),6);
% subset_offset_values(1:chunk_size,1) = rand_negatives;
% subset_offset_values(1*chunk_size:2*chunk_size-1,1) = rand_positives;

% % Generate random y values
% rand_negatives = truncate(-100 * rand([chunk_size 1]),6);
% rand_positives = truncate(100 * rand([chunk_size 1]),6);
% subset_offset_values(2*chunk_size:3*chunk_size-1,2) = rand_negatives;
% subset_offset_values(3*chunk_size:4*chunk_size-1,2) = rand_positives;

% % Generate random z values
% rand_negatives = truncate(-100 * rand([chunk_size 1]),6);
% rand_positives = truncate(100 * rand([chunk_size 1]), 6);
% subset_offset_values(4*chunk_size:5*chunk_size-1,3) = rand_negatives;
% subset_offset_values(5*chunk_size:6*chunk_size-1,3) = rand_positives;

% % Generate some NaN offsets
% subset_offset_values(6*chunk_size:7*chunk_size-1,:) = NaN([chunk_size 3]);

% % Generate sets of random offsets
% rand_nums = 200*rand([chunk_size,3]) - 100;
% subset_offset_values(7*chunk_size:8*chunk_size-1,:) = truncate(rand_nums,6);

% % Generate random positive and negative timedeltas
% subset_timedelta(8*chunk_size:9*chunk_size-1) = truncate(-1 * rand([chunk_size 1]),3);
% subset_timedelta(9*chunk_size:10*chunk_size-1) = truncate(rand([chunk_size 1]),3);

% % Set some quality flags
% subset_quality_flags(10*chunk_size:11*chunk_size-1) = 1;

% % Randomly set some quality flags
% subset_quality_flags(11*chunk_size:12*chunk_size-1) = round(rand([chunk_size 1]));

% % Set each quality bitmask
% small_chunk = floor(chunk_size/7);
% chunk_start = 12*chunk_size;
% for i = 1:7
% subset_quality_bitmask(chunk_start:chunk_start + small_chunk - 1) = bitshift(1,i);
% chunk_start = chunk_start + small_chunk;
% end

% % Set some random quality bitmasks
% subset_quality_bitmask(chunk_start:chunk_start+chunk_size-1) = round(rand([chunk_size 1])*255);

% offset_values(all_in_day_idx, :) = subset_offset_values;
% timedelta(all_in_day_idx) = subset_timedelta;
% quality_flags(all_in_day_idx) = subset_quality_flags;
% quality_bitmask(all_in_day_idx) = subset_quality_bitmask;

Metadata.dependencies = [];
Metadata.science = {sciencefile};
Metadata.creation_timestamp = string(datetime("now"), 'yyyy-MM-dd''T''HH:mm:ss');
Metadata.comment = "This is a zero value calibration file";
generateCalFile(epoch, offset_values, timedelta, quality_flags, quality_bitmask, "noop", Metadata, calfile)

end

function n= truncate(num,digits)
n=fix(num*10^digits)/10^digits;
end

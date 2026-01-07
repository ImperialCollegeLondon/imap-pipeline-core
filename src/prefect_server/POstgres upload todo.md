# Postgres upload todo

- test if it works

PGPASSWORD=postgres psql -h host.docker.internal -U postgres -d imap

INSERT INTO public.files(
    name, path, version, hash, content_date, software_version, size, creation_date, last_modified_date, deletion_date)
    VALUES ('imap_mag_l1_hsk-procstat_20251101_v001.csv', 'hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv', 1, 0, '2025-11-01', 1, 9999, '2025-11-02', '2025-11-02', NULL);

INSERT INTO public.files(
    name, path, version, hash, content_date, software_version, size, creation_date, last_modified_date, deletion_date)
    VALUES ('imap_mag_l1_hsk-procstat_20251102_v001.csv', 'hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251102_v001.csv', 1, 0, '2025-11-02', 1, 9999, '2025-11-03', '2025-11-03', NULL);

INSERT INTO public.files(
    name, path, version, hash, content_date, software_version, size, creation_date, last_modified_date, deletion_date)
    VALUES ('imap_mag_l1_hsk-procstat_20251102_v002.csv', 'hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251102_v002.csv', 2, 0, '2025-11-02', 1, 9999, '2025-11-04', '2025-11-04', NULL);

delete from workflow_progress where item_name='postgres-upload';

- make it so database that is uploaded to can be changed/run more than once
- add find_files_before filter?

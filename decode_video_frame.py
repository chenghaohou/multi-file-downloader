# coding: utf-8

import os
import sys
import subprocess
import multiprocessing


def decode_frame(frame_path, video_file_path):
    try:
        if os.path.exists(frame_path):
            if not os.path.exists(os.path.join(frame_path, 'image_00001.jpg')):
                subprocess.call('rm -r {}'.format(frame_path), shell=True)
                print('remove {}'.format(frame_path))
                os.mkdir(frame_path)
        else:
            os.mkdir(frame_path)
        # cmd = 'ffmpeg -i {} -vf scale=-1:360 {}/image_%05d.jpg'.format(video_file_path, frame_path)

        # ffmpeg -i 33350141-102-9987625-053746.mp4 -vf select='eq(pict_type\,I)' -vsync 2  -f image2 core-%02d.jpeg
        # 仅解码视频I帧
        cmd = 'ffmpeg -i {} -vf select=\'eq(pict_type\,I)\' -vsync 2 -f image2 {}/image_%05d.jpg'.format(video_file_path, frame_path)
        print('Processing: ', cmd)   
        subprocess.call(cmd, shell=True)
    except:
        print('Error video: ', video_file_path)


def decode_process(data_dir, video_files, pool_size):
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)
    pool = multiprocessing.Pool(processes=pool_size)
    for video in video_files:
        video_name = os.path.split(video)[-1]
        frame_folder, _ = os.path.splitext(video_name)
        frame_path = os.path.join(data_dir, frame_folder)
        pool.apply_async(decode_frame, args=(frame_path, video))
    pool.close()
    pool.join()


def main(video_dir, frame_dir, file_to_decode):
    to_decode_list = []
    decode_video = []
    with open(file_to_decode) as f:
        for line in f:
            line_split = line.strip().split(';')
            guid = line_split[1]
            video_file = guid + '.mp4'
            video_path = os.path.join(video_dir, video_file)
            if os.path.exists(video_path):
                to_decode_list.append(video_path)
                decode_video.append(line)
            else:
                print('None: ', video_path)
    
    with open(file_to_decode + '_decoded', 'w') as f:
        for line in decode_video:
            f.write(line)

    MAX_POOL_SIZE = 30  # 可以自行调整最大进程池的大小，默认30
    pool_size = min(MAX_POOL_SIZE, len(to_decode_list))
    decode_process(frame_dir, to_decode_list, pool_size)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python decode_video_frame.py video_path frame_path decode_video_file")
    main(sys.argv[1], sys.argv[2], sys.argv[3])

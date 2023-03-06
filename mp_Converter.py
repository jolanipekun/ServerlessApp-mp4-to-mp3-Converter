from moviepy.editor import *

mp4_file = "awws.mp4"

mp3_file = "audio.mp3"

video = VideoFileClip(mp4_file)

video.audio.write_audiofile("audio.mp3")
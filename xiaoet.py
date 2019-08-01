"""xiaoet donload main script"""

import os
import logging
import asyncio
import httpx
from collections import namedtuple
from m3u8 import loads
from m3u8.model import SegmentList, Segment, find_key

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

SingleDownloadResult = namedtuple("SingleDownloadResult", "success reason data_url")


class XET(object):
    APPID = ''  # APPid
    RESOURCEID = ''  # ResourceID，这里的resourceid代表课程id
    sessionid = ''  # Cookie laravel_session
    client = httpx.AsyncClient()
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        'Referer': '',
        'Origin': 'http://pc-shop.xiaoe-tech.com',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    cookie = {
        'laravel_session': sessionid
    }
    
    async def get_lesson_list(self):
        url = 'https://pc-shop.xiaoe-tech.com/{appid}/open/column.resourcelist.get/2.0'.format(appid=self.APPID)
        body = {
            'data[page_index]': '0',
            'data[page_size]': '1000',
            'data[order_by]': 'start_at:desc',
            'data[resource_id]': self.RESOURCEID,
            'data[state]': '0'
        }
        # 获取当前课程的信息
        self.header['Referer'] = 'https://pc-shop.xiaoe-tech.com/{appid}/'.format(appid=self.APPID)
        logger.debug("ready to send post request")
        resp = await self.client.post(url, data=body, headers=self.header, cookies=self.cookie)
        logger.debug("response: {}".format(resp))
        if resp.status_code != 200:
            raise Exception('获取课程列表失败')
        try:
            # 拼接课程id、标题以及资源类型
            data = [{'id': lesson['id'], 'name': lesson['title'], 'resource_type': lesson['resource_type']} for lesson
                    in resp.json()['data']]
        except:
            logger.exception("get lesson list failed.")
            raise SystemExit("获取课程列表失败")
        # 返回课程列表
        return data
    
    async def get_lesson_hls(self, resource):
        """
        :param resource: 这里的resource代表当前课程下的某节课的id
        :return:
        """
        resource_type = {'2': 'audio.detail.get', '3': 'video.detail.get'}
        url = 'https://pc-shop.xiaoe-tech.com/{appid}/open/{resource}/1.0'.format(appid=self.APPID,
                                                                                  resource=resource_type[
                                                                                      str(resource['resource_type'])])
        body = {
            'data[resource_id]': resource['id']
        }
        self.header['Referer'] = 'https://pc-shop.xiaoe-tech.com/{appid}/video_details?id={resourceid}'.format(
            appid=self.APPID, resourceid=self.RESOURCEID)
        resp = await self.client.post(url, data=body, headers=self.header, cookies=self.cookie)
        if resp.status_code != 200:
            raise Exception('获取课程信息失败')
        # 返回当前课程的信息
        hls = resp.json()['data']
        return hls
    
    async def fetch_single_ts_file(self, url, ts_file, semaphore):
        """
        Download single ts file, used when download videos.
        :param url: full path of ts file
        :param ts_file: file pull save path.
        :param semaphore: semaphore for coroutines synchronizing
        :return:
        """
        result = SingleDownloadResult(True, "", url)
        async with semaphore:
            # 下载ts文件
            try:
                resp = await self.client.get(url, headers=self.header, cookies=self.cookie)
            except Exception as e:
                result.success = False
                result.reason = e
                return result
            if resp.status_code != 200:
                result.success = False
                result.reason = "status code error, expected: 200, actually: {}".format(resp.status_code)
                return result
            # 如果文件不存在或者本地文件大小于接口返回大小不一致则保存ts文件
            if not os.path.exists(ts_file) or os.stat(ts_file).st_size != resp.headers['content-length']:
                with open(ts_file, 'wb') as ts:
                    ts.write(resp.content)
        return result
    
    async def video(self, url, media_dir, title, playurl):
        """
        :param url: hls 视频流文件
        :param media_dir: 下载保存目录
        :param title:  视频标题
        :param playurl: ts文件地址
        :return:
        """
        resp = await self.client.get(url, headers=self.header)
        
        media = loads(resp.text)
        # 拼接ts文件列表
        playlist = ["{playurl}{uri}".format(playurl=playurl, uri=uri) for uri in media.segments.uri]
        
        n = 0
        new_segments = []
        semaphore = asyncio.BoundedSemaphore(20)
        tasks = []
        # get ts file list
        # async with self.client.parallel() as parallel:
        for url in playlist:
            ts_file = os.path.join(media_dir, title, 'm_{num}.ts'.format(num=n))
            ts_path = os.path.join(title, 'm_{num}.ts'.format(num=n))
            media.data['segments'][n]['uri'] = ts_path
            new_segments.append(media.data.get('segments')[n])
            tasks.append(asyncio.ensure_future(self.fetch_single_ts_file(url, ts_file, semaphore)))
            
            n += 1
        
        results = await asyncio.gather(*tasks)
        for download_result in results:
            if not download_result.success:
                logger.exception("url: {} download failed, reason: {}".format(download_result.data_url,
                                                                              download_result.reason))
        # change m3u8 data
        media.data['segments'] = new_segments
        # 修改m3u8文件信息
        segments = SegmentList(
            [Segment(base_uri=None, keyobject=find_key(segment.get('key', {}), media.keys), **segment)
             for segment in
             media.data.get('segments', [])])
        media.segments = segments
        
        # save m3u8 file
        m3u8_file = os.path.join(media_dir, '{title}.m3u8'.format(title=title))
        if not os.path.exists(m3u8_file):
            with open(m3u8_file, 'w', encoding='utf8') as f:
                f.write(media.dumps())
    
    async def audio(self, url, media_dir, title):
        # 下载音频
        resp = await self.client.get(url, headers=self.header, stream=True)
        if resp.status_code != 200:
            print('Error: {title}'.format(title=title))
        else:
            audio_file = os.path.join(media_dir, title, '{title}.mp3'.format(title=title))
            if not os.path.exists(audio_file):
                with open(audio_file, 'wb') as f:
                    f.write(resp.content)
    
    def download(self):
        # 设置保存目录
        media_dir = 'media'
        # 获取课程信息
        logger.debug("ready to get lesson list")
        lesson_list = asyncio.get_event_loop().run_until_complete(self.get_lesson_list())
        logger.debug("lesson list: {}".format(lesson_list))
        for resourceid in lesson_list:
            # 课程类型为1和6的直接跳过
            if resourceid['resource_type'] == 1 or resourceid['resource_type'] == 6:
                continue
            
            data = asyncio.get_event_loop().run_until_complete(self.get_lesson_hls(resourceid))
            title = data['title']
            # 判断media目录是否存在
            if not os.path.exists(media_dir):
                os.mkdir(media_dir)
            # 课程类型为2则代表音频，可直接下载
            if resourceid['resource_type'] == 2:
                playurl = data['audio_url']
                
                if not os.path.exists(os.path.join(media_dir, title)):
                    try:
                        os.mkdir(os.path.join(media_dir, title))
                    except OSError as e:
                        title = title.replace('|', '丨')
                        os.mkdir(os.path.join(media_dir, title))
                asyncio.get_event_loop().run_until_complete(self.audio(playurl, media_dir, title))
            
            # 课程类型为3则代表视频下载后需要手动拼接
            elif resourceid['resource_type'] == 3:
                url = data['video_hls']
                playurl = url.split('v.f230')[0]
                
                # mkdir media directory
                if not os.path.exists(os.path.join(media_dir, title)):
                    os.mkdir(os.path.join(media_dir, title))
                
                asyncio.get_event_loop().run_until_complete(self.video(url, media_dir, title, playurl))


if __name__ == '__main__':
    asyncio.get_event_loop().set_debug(True)
    XET().download()

package sstable

import (
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	SegmentSize = 8*1024 ^ 2
)

func (s *SSTable) searchAndMergeSegments() {

	l := len(s.Segments)
	if l < 3 {
		return
	}
	for i := 1; i < l; i++ {

		//将文件大小不足额定大小一半的文件合并到其上一个文件中
		if s.Segments[i].size < SegmentSize/2 {

			err := s.merge(i-1, i)
			if err != nil {
				log.Println(fmt.Errorf("merge failed,%w", err))
				continue
			}
		}
	}

}

func (s *SSTable) merge(firstIndex, secondIndex int) error {

	newSeg, err := newTmpSegment(s.Root, s.Segments[secondIndex].id)
	if err != nil {
		return err
	}

	fSeg := s.Segments[firstIndex]
	sSeg := s.Segments[secondIndex]

	fSeg.newscanner()
	sSeg.newscanner()

	firstScan := fSeg.scan()
	secondScan := sSeg.scan()

	for firstScan || secondScan {
		firstChunk := fSeg.scanValue()
		secondChunk := sSeg.scanValue()

		if !firstScan || secondChunk.Key < firstChunk.Key {
			if err := newSeg.write(secondChunk); err != nil {
				return errors.New("merge failed")
			}
			secondScan = sSeg.scan()
			continue
		}

		if !secondScan || firstChunk.Key < secondChunk.Key {
			if err := newSeg.write(firstChunk); err != nil {
				return errors.New("merge failed")
			}
			firstScan = fSeg.scan()
			continue
		}

		if firstChunk.Key == secondChunk.Key {
			if err := newSeg.write(secondChunk); err != nil {
				return errors.New("merge failed")
			}
			firstScan = fSeg.scan()
			secondScan = sSeg.scan()
		}
	}
	newSeg.sync()

	//todo: lock
	s.Segments[firstIndex].delete()
	s.Segments[secondIndex].delete()
	newSeg.turnToNormal()
	s.Segments[secondIndex] = newSeg
	s.Segments = append(s.Segments[:firstIndex], s.Segments[secondIndex:]...)

	return nil
}

func (s *SSTable) startMergeMonitor() {
	for {
		select {
		case <-time.After(time.Duration(5) * time.Second):
			s.searchAndMergeSegments()
		case <-s.ctx.Done():
			return
		}
	}
}

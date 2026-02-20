import re


def getSpeechBlocks(whispered, silence_time=0.8):
    text_blocks, (st, et, txt) = [], (0, 0, "")
    for i, seg in enumerate(whispered["segments"]):
        if seg["start"] - et > silence_time:
            if txt:
                text_blocks.append([[st, et], txt])
            (st, et, txt) = (seg["start"], seg["end"], seg["text"])
        else:
            et, txt = seg["end"], txt + seg["text"]

    if txt:
        text_blocks.append([[st, et], txt])  # For last text block

    return text_blocks


def cleanWord(word):
    return re.sub(r'[^\w\s\-_"\'\']', "", word)


def interpolateTimeFromDict(word_position, d):
    for key, value in d.items():
        if key[0] <= word_position <= key[1]:
            return value
    return None


def getTimestampMapping(whisper_analysis):
    index = 0
    locationToTimestamp = {}
    for segment in whisper_analysis["segments"]:
        for word in segment["words"]:
            newIndex = index + len(word["text"]) + 1
            locationToTimestamp[(index, newIndex)] = word["end"]
            index = newIndex
    return locationToTimestamp


def splitWordsBySize(words, maxCaptionSize):
    halfCaptionSize = maxCaptionSize / 2
    captions = []
    while words:
        caption = words[0]
        words = words[1:]
        while words and len(caption + " " + words[0]) <= maxCaptionSize:
            caption += " " + words[0]
            words = words[1:]
            if len(caption) >= halfCaptionSize and words:
                break
        captions.append(caption)
    return captions


def getWordByWordCaptionsWithTime(transcriptions):
    word_captions = []
    # Process each segment with word-level timing
    for segment in transcriptions["segments"]:
        if "words" not in segment:
            continue

        for word in segment["words"]:
            word_text = word["text"].strip()
            if word_text:  # Skip empty words
                start_time = word["start"]
                end_time = word["end"]
                word_captions.append(((start_time, end_time), word_text))

    return word_captions


def getCaptionsWithTime(transcriptions, maxCaptionSize=15, considerPunctuation=True):
    time_splits = []
    current_caption = []
    current_length = 0

    # Ensure we only work with transcriptions that have word-level timing
    segments = [seg for seg in transcriptions["segments"] if "words" in seg]

    # Flatten all words from all segments
    all_words = []
    for segment in segments:
        all_words.extend(segment["words"])

    for i, word in enumerate(all_words):
        word_text = word["text"]

        # Check if this word would exceed maxCaptionSize
        new_length = current_length + \
            len(word_text) + (1 if current_caption else 0)

        # Determine if we should split here
        should_split = (
            new_length > maxCaptionSize
            or (
                considerPunctuation
                and word_text.rstrip(".,!?") != word_text
                and current_caption
            )
            or i == len(all_words) - 1
            or len(current_caption) >= 5
        )

        # Add word to current caption if we're not splitting yet
        if not should_split:
            current_caption.append(word_text)
            current_length = new_length
            continue

        # Handle the split
        if current_caption:
            # Add current word if this is the last one
            if i == len(all_words) - 1 and new_length <= maxCaptionSize:
                current_caption.append(word_text)

            caption_text = " ".join(current_caption)
            start_time = all_words[i - len(current_caption)]["start"]
            end_time = (
                word["end"] if word_text in current_caption else all_words[i - 1]["end"]
            )
            time_splits.append(((start_time, end_time), caption_text))

        # Handle current word if it wasn't added to the previous caption
        if word_text not in current_caption and i == len(all_words) - 1:
            time_splits.append(((word["start"], word["end"]), word_text))

        # Reset for next caption
        current_caption = []
        current_length = 0

        # Start new caption with current word if it wasn't the last one
        if i < len(all_words) - 1:
            current_caption.append(word_text)
            current_length = len(word_text)

    return time_splits

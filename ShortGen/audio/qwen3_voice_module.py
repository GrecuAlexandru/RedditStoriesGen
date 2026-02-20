import os
import torch
import soundfile as sf
from qwen_tts import Qwen3TTSModel
from ShortGen.audio.voice_module import VoiceModule

# Resolve the local model path relative to this file's location
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_LOCAL_MODEL_PATH = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "models"))
# Default reference voice clip for voice cloning
_DEFAULT_REF_AUDIO = os.path.normpath(
    os.path.join(_THIS_DIR, "..", "..", "base_voice.mp3")
)


class Qwen3VoiceModule(VoiceModule):
    def __init__(self, model_id=None, language="English", ref_audio=None):
        super().__init__()
        self.language = language
        self.model_id = model_id if model_id is not None else _LOCAL_MODEL_PATH
        self.ref_audio = ref_audio if ref_audio is not None else _DEFAULT_REF_AUDIO

        if not os.path.exists(self.ref_audio):
            raise FileNotFoundError(
                f"Reference audio not found: {self.ref_audio}\n"
                "Please place a voice clip at base_voice.mp3 in the project root."
            )

        device = "cuda:0" if torch.cuda.is_available() else "cpu"
        dtype = torch.bfloat16 if torch.cuda.is_available() else torch.float32

        print(f"Loading Qwen3-TTS model from {self.model_id} on {device}...")
        self.model = Qwen3TTSModel.from_pretrained(
            self.model_id,
            device_map=device,
            dtype=dtype,
        )
        print("Model loaded successfully.")
        print(f"Pre-computing speaker embedding from: {self.ref_audio}")
        # Pre-compute the voice clone prompt once so it's fast per-generation
        self._voice_prompt = self.model.create_voice_clone_prompt(
            ref_audio=self.ref_audio,
            x_vector_only_mode=True,  # No reference transcript needed
        )
        print("Speaker embedding ready.")

    def generate_voice(self, text, outputfile):
        print(f"Generating audio with Qwen3-TTS for text ({len(text)} chars)...")
        import re
        import numpy as np

        # Split text into chunks (by sentences) to avoid massive single generations
        sentences = re.split(r"(?<=[.!?\n]) +", text.strip())
        chunks = []
        current_chunk = ""
        for s in sentences:
            if len(current_chunk) + len(s) > 200 and current_chunk:
                chunks.append(current_chunk)
                current_chunk = s
            else:
                current_chunk += (" " if current_chunk else "") + s
        if current_chunk:
            chunks.append(current_chunk)

        all_wavs = []
        out_sr = 24000

        for i, chunk in enumerate(chunks):
            chunk = chunk.strip()
            if not chunk:
                continue
            print(
                f"  -> Processing chunk {i + 1}/{len(chunks)} ({len(chunk)} chars): {chunk[:60].replace(chr(10), ' ')}..."
            )
            wavs, sr = self.model.generate_voice_clone(
                text=chunk,
                language=self.language,
                voice_clone_prompt=self._voice_prompt,
            )
            all_wavs.append(wavs[0])
            out_sr = sr

        if not all_wavs:
            raise ValueError("TTS did not generate any audio.")

        final_wav = np.concatenate(all_wavs)
        sf.write(outputfile, final_wav, out_sr)
        print(f"Audio saved to {outputfile}")
        return outputfile

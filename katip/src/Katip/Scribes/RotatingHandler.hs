module Katip.Scribes.RotatingHandler where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Data.ByteString.Lazy   as BL
import Data.IORef
import GHC.IO.Handle
import GHC.IO.Handle.FD
import GHC.IO.IOMode

-- | File owner struct writing data to the file
-- sequentially. Automatic buffer flushing and log rotating.
data FileOwner = FileOwner
  { foDataQueune   :: TBQueue BL.ByteString
  , foControlQueue :: TBQueue ControlMsg
  , foAsync        :: Async ()
    -- ^ Wait for to be sure that worker thread is closed.
  }

data FileOwnerSettings = FileOwnerSettings
  { fosDebounceFreq    :: Maybe Int
  , fosDataQueueLen    :: Int
  , fosControlQueueLen :: Int
  }

defaultFileOwnerSettings :: FileOwnerSettings
defaultFileOwnerSettings = FileOwnerSettings
  { fosDebounceFreq = 200000 -- every 200ms
  , fosDataQueueLen = 1000
  , fosControlQueueLen = 100
  }

data ControlMsg
  = CloseMsg
  | FlushMsg
  | ReopenMsg
  deriving (Eq, Ord, Show)

newFileOwner :: FilePath -> FileOwnerSettings -> IO FileOwner
newFileOwner fp s = do
  dqueue <- newTBQueueIO $ fosDataQueueLen s
  cqueue <- newTBQueueIO $ fosControlQueueLen s
  let
    newResource = openBinaryFile fp AppendMode
    ack = newResource >>= newIORef
    release ref = do
      h <- readIORef ref
      hFlush h
      hClose h
    go ref = do
      let
        readAllData = do
          a <- readTBQueue dqueue
          -- flushTBQueue never retries
          as <- flushTBQueue dqueue
          return $ a:as
        readMsg
          =   (Left <$> readTBQueue cqueue)
          <|> (Right <$> readAllData)
      atomically readMsg >>= \case
        Right bs -> do
          h <- readIORef ref
          BL.hPutStr h $ mconcat bs
          -- flush ref
          go ref
        Left c -> case c of
          CloseMsg -> return ()
          FlushMsg -> do
            h <- readIORef ref
            hFlush h
            go ref
          ReopenMsg -> do
            newH <- newResource
            oldH <- atomicModifyIORef' ref (\oldH -> (newH, oldH))
            hFlush oldH
            hClose oldH
            go ref
    worker :: IO ()
    worker = bracket ack release go
  asyncRet <- async worker
  return $ FileOwner
    { foDataQueune   = dqueue
    , foControlQueue = cqueue
    , foAsync        = asyncRet
    }



fileOwnerControl :: FileOwner -> ControlMsg -> IO ()
fileOwnerControl fo msg = do
  atomically $ writeTBQueue (foControlQueue fo) msg
  when (msg == CloseMsg) $ do
    -- Wait for worker thread to finish
    void $ waitCatch $ foAsync fo

writeFileOwner :: FileOwner -> BL.ByteString -> IO ()
writeFileOwner fo bs = do
  a <- return $!! bs
  -- The deepseq here is to be sure that writing thread will not
  -- calculate thunks and will not get into the blackhole or
  -- something. Precalculating is the responsibility of the sending
  -- thread, because there may be several sending threads and only one
  -- writing.
  atomically $ writeTBQueue (foDataQueune fo) $!! a

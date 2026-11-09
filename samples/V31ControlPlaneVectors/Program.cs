using System.Security.Cryptography;
using System.Text;
using BinSync;

byte[] seed = Convert.FromHexString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
byte[] publicKey = Convert.FromHexString("03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8");
byte[] nodeBytes = Convert.FromHexString("01000000000000000000000000000000");
NodeId node = NodeId.From(nodeBytes);
byte[] zero32 = new byte[32];
byte[] fullReplaceDigest = Enumerable.Repeat((byte)0x22, 32).ToArray();
Guid generation = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");

try
{
    var roster = new NamespaceRosterCertificate
    {
        NamespaceId = 7,
        RosterGeneration = 1,
        PreviousRosterDigest = zero32,
        AuthorityNodeId = node,
        Members =
        [
            new NamespaceRosterMember
            {
                NodeId = node,
                PublicKey = publicKey,
                TransportPeerEpoch = 1,
                Status = NamespaceMemberStatus.Active,
                RetiredThroughEpoch = 0
            }
        ],
        RetiredOriginEpochs = Array.Empty<NamespaceRetiredOriginEpoch>()
    };

    byte[] rosterPayload = NamespaceRosterProtocol.EncodePayload(roster);
    ExpectHex(rosterPayload,
        "4e5253320700010000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000001000100000000000000000000000000000003a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b80100000001000000000000",
        "NRS2 payload");
    byte[] rosterSigned = NamespaceRosterProtocol.Sign(roster, seed);
    ExpectHex(rosterSigned,
        "4e5253320700010000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000001000100000000000000000000000000000003a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b80100000001000000000000b27ce494fdde7f1236bd626bc02c3ced5573cecc557298093089e0f911c3f4d9f6d13ebb600918ab5fad3a8004c27fe1511b6f768c6ad62b3f9250b651640500",
        "NRS2 signed");
    byte[] rosterDigest = NamespaceRosterProtocol.ComputeDigest(roster);
    ExpectHex(rosterDigest,
        "0672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe",
        "NRS2 digest");
    VerifySignature(rosterSigned, 123, "BIN-SYNC/NAMESPACE-ROSTER/v2", publicKey, "NRS2");

    var frontier = new AppliedOriginFrontierReport
    {
        NamespaceId = 7,
        RosterGeneration = 1,
        RosterDigest = rosterDigest,
        MemberNodeId = node,
        MemberTransportEpoch = 1,
        ReportSequence = 1,
        FinalForAuthor = false,
        UnpublishedDraftCount = 0,
        MaxUnpublishedDraftHlcWall = 0,
        MaxUnpublishedDraftHlcLogical = 0,
        Entries =
        [
            new AppliedOriginFrontierEntry
            {
                OriginAuthorNodeId = node,
                OriginAuthorEpoch = 1,
                ContiguousAppliedOriginSequence = 1
            }
        ],
        IssuedOrigins =
        [
            new IssuedOriginFrontierEntry
            {
                OriginAuthorEpoch = 1,
                IssuedOriginSequence = 1,
                MaxIssuedHlcWall = 100,
                MaxIssuedHlcLogical = 0
            }
        ]
    };
    byte[] frontierPayload = OriginFrontierProtocol.EncodePayload(frontier);
    ExpectHex(frontierPayload,
        "4f465232070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe010000000000000000000000000000000100000001000000000000000000000000000000000000000000000000000000000100010000000000000000000000000000000100000001000000000000000100010000000100000000000000640000000000000000000000",
        "OFR2 payload");
    byte[] frontierSigned = OriginFrontierProtocol.Sign(frontier, seed);
    ExpectHex(frontierSigned,
        "4f465232070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe0100000000000000000000000000000001000000010000000000000000000000000000000000000000000000000000000001000100000000000000000000000000000001000000010000000000000001000100000001000000000000006400000000000000000000008436fa07da6cddd101a7624abbedec43484029df97bedf813d6eb1c6dbb0e33d381786ac6a22918f977c2fb86a9d80031d82386fbeae459876b981dc938fd805",
        "OFR2 signed");
    VerifySignature(frontierSigned, 151, "BIN-SYNC/APPLIED-ORIGIN-FRONTIER/v2", publicKey, "OFR2");

    var checkpoint = new NamespaceRetentionCheckpoint
    {
        NamespaceId = 7,
        RosterGeneration = 1,
        RosterDigest = rosterDigest,
        CheckpointSequence = 1,
        PreviousCheckpointDigest = zero32,
        SafeFrontier =
        [
            new RetentionFrontierEntry
            {
                OriginAuthorNodeId = node,
                OriginAuthorEpoch = 1,
                SafeAppliedOriginSequence = 1
            }
        ],
        FullReplaceGeneration = generation,
        FullReplaceDigest = fullReplaceDigest,
        FullReplaceHlcWallFloor = 100,
        FullReplaceHlcLogicalFloor = 2
    };
    byte[] checkpointPayload = NamespaceRetentionCheckpointProtocol.EncodePayload(checkpoint);
    ExpectHex(checkpointPayload,
        "52435032070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe010000000000000000000000000000000000000000000000000000000000000000000000000000000100010000000000000000000000000000000100000001000000000000000100112233445566778899aabbccddeeff2222222222222222222222222222222222222222222222222222222222222222640000000000000002000000",
        "RCP2 payload");
    byte[] checkpointSigned = NamespaceRetentionCheckpointProtocol.Sign(checkpoint, seed);
    ExpectHex(checkpointSigned,
        "52435032070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe010000000000000000000000000000000000000000000000000000000000000000000000000000000100010000000000000000000000000000000100000001000000000000000100112233445566778899aabbccddeeff22222222222222222222222222222222222222222222222222222222222222226400000000000000020000005a44c4244802d49e440a39b1221456b5ab89b0290240cc017ac5692029d4702318735c49327879c90527514eedd0b6d23c0d86ed278d0d88188ed4e8c7bb5107",
        "RCP2 signed");
    VerifySignature(checkpointSigned, 177, "BIN-SYNC/RETENTION-CHECKPOINT/v2", publicKey, "RCP2");

    var installedAck = new FullReplaceInstalledAck
    {
        NamespaceId = 7,
        RosterGeneration = 1,
        RosterDigest = rosterDigest,
        Generation = generation,
        Digest = fullReplaceDigest,
        MemberNodeId = node,
        MemberTransportEpoch = 1,
        HlcWallFloor = 100,
        HlcLogicalFloor = 2,
        Installed = true
    };
    byte[] ackPayload = FullReplaceInstalledAckProtocol.EncodePayload(installedAck);
    ExpectHex(ackPayload,
        "46524132070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe00112233445566778899aabbccddeeff2222222222222222222222222222222222222222222222222222222222222222010000000000000000000000000000000100000064000000000000000200000001",
        "FRA2 payload");
    byte[] ackSigned = FullReplaceInstalledAckProtocol.Sign(installedAck, seed);
    ExpectHex(ackSigned,
        "46524132070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe00112233445566778899aabbccddeeff2222222222222222222222222222222222222222222222222222222222222222010000000000000000000000000000000100000064000000000000000200000001a069973a84f277ea298c7e07cf8fafda8305f50dd9bc0034cf32a41aba23a7775c7fd55af4487a41b2166d5cde30a35a93dc506a6ade9b85bb27499d97585b0e",
        "FRA2 signed");
    VerifySignature(ackSigned, 127, "BIN-SYNC/FULL-REPLACE-INSTALLED-ACK/v2", publicKey, "FRA2");

    var begin = new FullReplaceTransferProtocol.Begin(
        7, generation, 1, rosterDigest, 5, 1,
        Enumerable.Repeat((byte)0x33, 32).ToArray(), Array.Empty<byte>());
    var signedBegin = FullReplaceTransferProtocol.SignBegin(begin, seed);
    byte[] beginEncoded = FullReplaceTransferProtocol.EncodeBegin(signedBegin);
    ExpectHex(beginEncoded,
        "070000112233445566778899aabbccddeeff01000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe0500000000000000010000003333333333333333333333333333333333333333333333333333333333333333c1aa9215a61eeb2b5ef8abd371ed23b1fda68fda1a171b5918d3e5f00e1f624133d885e8482a2e15ae511e3f52a87e827d3129d838d7a106b7e977f998021f0a",
        "FULL_REPLACE_BEGIN signed");
    VerifySignature(beginEncoded, 102, "BIN-SYNC/FULL-REPLACE-MANIFEST/v1", publicKey, "FULL_REPLACE_BEGIN");

    var chunkMutation = new ProviderMutation
    {
        RecordId = RecordId.From(Convert.FromHexString("02000000000000000000000000000000")),
        FieldId = 1,
        HlcWall = 100,
        HlcLogical = 2,
        AuthorNodeId = node,
        Value = "abc"u8.ToArray(),
        ChangeId = BatchId.From(Convert.FromHexString("03000000000000000000000000000000")),
        OriginAuthorEpoch = 1,
        OriginSequence = 1
    };
    var chunk = new FullReplaceTransferProtocol.Chunk(7, generation, 0, [chunkMutation]);
    byte[] chunkEncoded = FullReplaceTransferProtocol.EncodeChunk(chunk);
    ExpectHex(chunkEncoded,
        "070000112233445566778899aabbccddeeff00000000010054000000020000000000000000000000000000000100006400000000000000020000000100000000000000000000000000000000000300000003000000000000000000000000000000010000000100000000000000616263",
        "FULL_REPLACE_CHUNK");
    ExpectHex(FullReplaceTransferProtocol.EncodeChunk(
            FullReplaceTransferProtocol.DecodeChunk(chunkEncoded)),
        "070000112233445566778899aabbccddeeff00000000010054000000020000000000000000000000000000000100006400000000000000020000000100000000000000000000000000000000000300000003000000000000000000000000000000010000000100000000000000616263",
        "FULL_REPLACE_CHUNK roundtrip");

    var ready = new FullReplaceTransferProtocol.Progress(
        7, generation, 1, 1, Enumerable.Repeat((byte)0x33, 32).ToArray(),
        false, FullReplaceTransferProtocol.GetNextFrameSequence(1));
    byte[] readyEncoded = FullReplaceTransferProtocol.EncodeProgress(ready);
    ExpectHex(readyEncoded,
        "070000112233445566778899aabbccddeeff01000000010000003333333333333333333333333333333333333333333333333333333333333333000300000000000000",
        "FULL_REPLACE_READY");
    ExpectHex(FullReplaceTransferProtocol.EncodeProgress(
            FullReplaceTransferProtocol.DecodeProgress(readyEncoded)),
        "070000112233445566778899aabbccddeeff01000000010000003333333333333333333333333333333333333333333333333333333333333333000300000000000000",
        "FULL_REPLACE_READY roundtrip");

    var transferId = new FullReplaceTransferProtocol.TransferId(7, generation);
    byte[] controlEncoded = FullReplaceTransferProtocol.EncodeTransferId(transferId);
    ExpectHex(controlEncoded,
        "070000112233445566778899aabbccddeeff",
        "FULL_REPLACE_COMMIT / ABORT");
    FullReplaceTransferProtocol.TransferId decodedControl =
        FullReplaceTransferProtocol.DecodeTransferId(controlEncoded);
    ExpectHex(FullReplaceTransferProtocol.EncodeTransferId(decodedControl),
        "070000112233445566778899aabbccddeeff",
        "FULL_REPLACE_COMMIT / ABORT roundtrip");

    Console.WriteLine("v3.1 signed control-plane and FULL_REPLACE golden vectors: PASS");
    return 0;
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception);
    return 1;
}

static void ExpectHex(ReadOnlySpan<byte> actual, string expectedHex, string name)
{
    byte[] expected = Convert.FromHexString(expectedHex);
    if (!actual.SequenceEqual(expected))
        throw new InvalidDataException($"{name} differs from the checked-in golden vector.");
}

static void VerifySignature(byte[] encoded, int payloadLength, string domain, byte[] publicKey, string name)
{
    if (encoded.Length != payloadLength + BatchSignature.SignatureSize)
        throw new InvalidDataException($"{name} has an unexpected signed length.");
    byte[] message = Encoding.UTF8.GetBytes(domain)
        .Concat(encoded.AsSpan(0, payloadLength).ToArray()).ToArray();
    if (!BatchSignature.Verify(publicKey, message, encoded.AsSpan(payloadLength)))
        throw new CryptographicException($"{name} signature does not verify.");
}

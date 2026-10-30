
#define WIN32_LEAN_AND_MEAN
#include <windows.h>


#include "net.h"

#include "logger.h"
#include "csimplescan.h"
#include "steamclient.h"

#include <cstddef>

#ifdef X64BITS
static_assert(offsetof(CNetPacket, m_pubData) == 16, "Wrong offset of CNetPacket::m_pubData on 64-bit");
static_assert(offsetof(CNetPacket, m_cubData) == 24, "Wrong offset of CNetPacket::m_cubData on 64-bit");
#else
static_assert(offsetof(CNetPacket, m_pubData) == 12, "Wrong offset of CNetPacket::m_pubData on 32-bit");
static_assert(offsetof(CNetPacket, m_cubData) == 16, "Wrong offset of CNetPacket::m_cubData on 32-bit");
#endif


namespace NetHook
{


BBuildAndAsyncSendFrameFn BBuildAndAsyncSendFrame_Orig = nullptr;
RecvPktFn RecvPkt_Orig = nullptr;

CNet::CNet() noexcept
	: m_RecvPktDetour(nullptr),
	  m_BuildDetour(nullptr)
{
	CSimpleScan steamClientScan(STEAMCLIENT_DLL);

	BBuildAndAsyncSendFrameFn pBuildFunc = nullptr;
	const bool bFoundBuildFunc = steamClientScan.FindFunction(
#ifdef X64BITS
		"\x48\x8B\xC4\x55\x48\x8D\x68\x00\x48\x81\xEC\x00\x00\x00\x00\x48\x89\x70\x00\x49\x8B\xF0\x48\x89\x78\x00\x4C\x89\x60",
		"xxxxxxx?xxx????xxx?xxxxxx?xxx",
#else
		"\x55\x8B\xEC\x83\xEC\xCC\xA1\xCC\xCC\xCC\xCC\x53\x8B\xD9\x89\x5D\xCC\x83\x38\xCC\x75\xCC\x33\xC0\xEB\xCC\x8D\x45\xCC\x50\x6A\xCC\x68\xCC\xCC\xCC\xCC\xFF\x15\xCC\xCC\xCC\xCC\x83\xC4\xCC\x83\x7B\xCC\x02",
		"xxxxx?x????xxxxx?xx?x?xxx?xx?xx?x????xx????xx?xx?x",
#endif
		(void**)&pBuildFunc
	);

	BBuildAndAsyncSendFrame_Orig = pBuildFunc;

	g_pLogger->LogConsole("CWebSocketConnection::BBuildAndAsyncSendFrame = 0x%p\n", BBuildAndAsyncSendFrame_Orig);

	RecvPktFn pRecvPktFunc = nullptr;
	const bool bFoundRecvPktFunc = steamClientScan.FindFunction(
#ifdef X64BITS
		"\x48\x8B\xC4\x55\x48\x8D\xA8\xCC\xCC\xCC\xCC\x48\x81\xEC\xCC\xCC\xCC\xCC\x48\x89\x58\x08\x48\x8B\xDA\x48\x89\x70\xCC\x48\x8B\xF1",
		"xxxxxxx????xxx????xxxxxxxxxx?xxx",
#else
		"\x55\x8B\xEC\x81\xEC\xCC\xCC\xCC\xCC\xA1\xCC\xCC\xCC\xCC\x56\x8B\xF1\x57",
		"xxxxx????x????xxxx",
#endif
		(void**)&pRecvPktFunc
	);

	RecvPkt_Orig = pRecvPktFunc;

	g_pLogger->LogConsole("CCMInterface::RecvPkt = 0x%p\n", RecvPkt_Orig);


	if (bFoundBuildFunc)
	{
		BBuildAndAsyncSendFrameFn thisBuildFunc = (BBuildAndAsyncSendFrameFn)CNet::BBuildAndAsyncSendFrame;

		m_BuildDetour = new CSimpleDetour((void **)&BBuildAndAsyncSendFrame_Orig, (void *)thisBuildFunc);
		m_BuildDetour->Attach();

		g_pLogger->LogConsole("Detoured CWebSocketConnection::BBuildAndAsyncSendFrame!\n");
	}
	else
	{
		g_pLogger->LogConsole("Unable to hook CWebSocketConnection::BBuildAndAsyncSendFrame: func scan failed.\n");
	}

	if (bFoundRecvPktFunc)
	{
		RecvPktFn thisRecvPktFunc = (RecvPktFn)CNet::RecvPkt;

		m_RecvPktDetour = new CSimpleDetour((void **)&RecvPkt_Orig, (void *)thisRecvPktFunc);
		m_RecvPktDetour->Attach();

		g_pLogger->LogConsole("Detoured CCMInterface::RecvPkt!\n");
	}
	else
	{
		g_pLogger->LogConsole("Unable to hook CCMInterface::RecvPkt: func scan failed.\n");
	}

}

CNet::~CNet()
{
	if (m_RecvPktDetour)
	{
		m_RecvPktDetour->Detach();
		delete m_RecvPktDetour;
	}

	if (m_BuildDetour)
	{
		m_BuildDetour->Detach();
		delete m_BuildDetour;
	}
}


bool CNet::BBuildAndAsyncSendFrame(
	void *webSocketConnection,
#ifndef X64BITS
	void *,
#endif
	EWebSocketOpCode eWebSocketOpCode, 
	const uint8 *pubData, 
	uint32 cubData)
{
	if (eWebSocketOpCode == EWebSocketOpCode::k_eWebSocketOpCode_Binary)
	{
		g_pLogger->LogNetMessage(ENetDirection::k_eNetOutgoing, pubData, cubData);
	}
	else
	{
		g_pLogger->LogConsole("Sending websocket frame with opcode %d (%s), ignoring\n",
			eWebSocketOpCode, EWebSocketOpCodeToName(eWebSocketOpCode)
		);
	}

	return (*BBuildAndAsyncSendFrame_Orig)(webSocketConnection, eWebSocketOpCode, pubData, cubData);
}

void CNet::RecvPkt(
	void *cmConnection,
#ifndef X64BITS
	void *,
#endif
	CNetPacket *pPacket)
{
	g_pLogger->LogNetMessage(ENetDirection::k_eNetIncoming, pPacket->m_pubData, pPacket->m_cubData);

	(*RecvPkt_Orig)(cmConnection, pPacket);
}


}
